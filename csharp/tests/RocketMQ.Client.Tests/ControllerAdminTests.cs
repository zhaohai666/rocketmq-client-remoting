// Wire-level proof of the controller admin calls: metadata discovery (1005), the sync-state-set
// read (1006, routed to the leader the metadata named), the controller config read (1010) and
// broadcast update (1009, last error wins), and the broker-data cleanup (1011, again through the
// leader, with the primitive invokeTime / isCleanLivingBroker fields always on the wire).
//
// The mock answers the metadata request with its own listener address as the leader, so the
// two-phase calls land twice on the recorded frame log: phase counts and phase bodies are what
// each case pins down.
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class ControllerAdminTests
{
    private static DefaultMQAdminExt Started(MockCluster cluster, string tag)
    {
        var admin = new DefaultMQAdminExt("ADMIN_ctrl_" + tag);
        admin.SetNamesrvAddr(cluster.NamesrvAddr);
        admin.Start();
        return admin;
    }

    private static WireRecord Single(MockCluster cluster, int code) =>
        Assert.Single(cluster.Records().FindAll(r => r.Code == code));

    private static List<string> KeysOf(WireRecord record) => new(record.Ext.Keys);

    private static void ScriptLeader(MockCluster cluster, string leaderAddr, bool isLeader = true)
    {
        cluster.SetReplyExt(RequestCode.ControllerGetMetadataInfo, req => new Dictionary<string, string>
        {
            ["group"] = "default",
            ["controllerLeaderId"] = "1",
            ["controllerLeaderAddress"] = leaderAddr,
            ["isLeader"] = isLeader ? "true" : "false",
            ["peers"] = "1: " + leaderAddr,
        });
    }

    // ---------------------------------------------------------------- in-sync state 1005 + 1006

    [Fact]
    public void GetInSyncStateData_AsksForMetadataFirst_ThenRoutesTheReadToTheLeader()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "sync_state");
        ScriptLeader(cluster, cluster.BrokerAddrs[0]);
        try
        {
            cluster.SetReplyBody(RequestCode.ControllerGetSyncStateData, req =>
                new BrokerReplicasInfo
                {
                    ReplicasInfoTable =
                    {
                        ["broker-a"] = new ReplicasInfo
                        {
                            MasterBrokerId = 0,
                            MasterAddress = "10.0.0.1:10911",
                            MasterEpoch = 12,
                            SyncStateSetEpoch = 34,
                        },
                    },
                }.Encode());

            BrokerReplicasInfo? info = admin.GetInSyncStateData(cluster.BrokerAddrs[0],
                new[] { "broker-a", "broker-b" });

            Assert.NotNull(info);
            ReplicasInfo replicas = Assert.Single(info!.ReplicasInfoTable).Value;
            Assert.Equal("10.0.0.1:10911", replicas.MasterAddress);
            Assert.Equal(12, replicas.MasterEpoch);
            Assert.Equal(34, replicas.SyncStateSetEpoch);
            Assert.Empty(replicas.InSyncReplicas);

            // Phase one: a bare metadata frame; phase two: the broker names ride in the body.
            WireRecord meta = Single(cluster, RequestCode.ControllerGetMetadataInfo);
            Assert.Empty(meta.Ext);
            Assert.False(meta.HasBody);
            WireRecord read = Single(cluster, RequestCode.ControllerGetSyncStateData);
            Assert.Empty(read.Ext);
            Assert.Equal("[\"broker-a\",\"broker-b\"]",
                Encoding.UTF8.GetString(read.Body));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetInSyncStateData_AReadRefusalOnTheLeaderIsABrokerException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "sync_state_err");
        ScriptLeader(cluster, cluster.BrokerAddrs[0]);
        cluster.SetReplyCode(RequestCode.ControllerGetSyncStateData, 16, "controller busy");
        try
        {
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.GetInSyncStateData(cluster.BrokerAddrs[0], new[] { "broker-a" }));
            Assert.Equal(16, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- controller config 1010/1009

    [Fact]
    public void GetControllerConfig_WithoutServersFallsBackToTheNameServerList()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "ctrl_config");
        try
        {
            cluster.SetReplyBody(RequestCode.GetControllerConfig,
                req => Encoding.UTF8.GetBytes("controllerType=raft\nnotifyBrokerRoleChanged=true\n"));

            Dictionary<string, PropertyMap>? config = admin.GetControllerConfig();

            WireRecord rec = Single(cluster, RequestCode.GetControllerConfig);
            // The fallback target is the name server itself: no ext, no body on the read.
            Assert.Empty(rec.Ext);
            Assert.False(rec.HasBody);
            Assert.Equal("raft", config![cluster.NamesrvAddr]["controllerType"]);
            Assert.Equal("true", config[cluster.NamesrvAddr]["notifyBrokerRoleChanged"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetControllerConfig_ARefusalFromAnyServerIsAClientException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "ctrl_config_err");
        cluster.SetReplyCode(RequestCode.GetControllerConfig, 1, "config store closed");
        try
        {
            MQClientException e = Assert.Throws<MQClientException>(() =>
                admin.GetControllerConfig(new[] { cluster.BrokerAddrs[0] }));
            Assert.Equal(1, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void UpdateControllerConfig_BroadcastsThePropertiesTextToEveryController()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "ctrl_update");
        try
        {
            var props = new PropertyMap { ["notifyBrokerRoleChanged"] = "false" };
            admin.UpdateControllerConfig(props,
                new[] { cluster.BrokerAddrs[0], cluster.BrokerAddrs[0] });

            Assert.Equal(2, cluster.CountRequests(RequestCode.UpdateControllerConfig));
            WireRecord rec = cluster.Records().FindAll(r => r.Code == RequestCode.UpdateControllerConfig)[0];
            Assert.Equal("notifyBrokerRoleChanged=false\n", Encoding.UTF8.GetString(rec.Body));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void UpdateControllerConfig_TheLastFailureIsTheOneThatSurfaces()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "ctrl_update_err");
        cluster.SetReplyCode(RequestCode.UpdateControllerConfig, 1, "rejected by quorum");
        try
        {
            var props = new PropertyMap { ["k"] = "v" };
            MQClientException e = Assert.Throws<MQClientException>(() =>
                admin.UpdateControllerConfig(props, new[] { cluster.BrokerAddrs[0] }));
            Assert.Equal(1, e.ResponseCode);
            Assert.Equal("rejected by quorum", e.Message);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void UpdateControllerConfig_WithNothingToWriteOrNowhereToSendItIsASilentNoop()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "ctrl_update_noop");
        try
        {
            admin.UpdateControllerConfig(new PropertyMap(), new[] { cluster.BrokerAddrs[0] });
            admin.UpdateControllerConfig(new PropertyMap { ["k"] = "v" }, new List<string>());
            Assert.Equal(0, cluster.CountRequests(RequestCode.UpdateControllerConfig));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- clean broker data 1011

    [Fact]
    public void CleanControllerBrokerData_CleansThroughTheLeader_WithThePrimitiveFieldsAlwaysOn()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "clean_broker");
        ScriptLeader(cluster, cluster.BrokerAddrs[0]);
        try
        {
            admin.CleanControllerBrokerData(cluster.BrokerAddrs[0], "MockCluster", "broker-a");

            WireRecord rec = Single(cluster, RequestCode.CleanBrokerData);
            Assert.Equal(
                new[] { "brokerControllerIdsToClean", "brokerName", "clusterName", "invokeTime",
                        "isCleanLivingBroker" },
                KeysOf(rec));
            Assert.Equal("MockCluster", rec.Ext["clusterName"]);
            Assert.Equal("broker-a", rec.Ext["brokerName"]);
            Assert.Equal("false", rec.Ext["isCleanLivingBroker"]);
            Assert.Equal("", rec.Ext["brokerControllerIdsToClean"]);
            // invokeTime is the wall clock of the request, not a constant: it only has to be
            // a plausible "now" (after 2020-01-01).
            Assert.True(long.Parse(rec.Ext["invokeTime"]) > 1577836800000L);
            Assert.False(rec.HasBody);

            // Both phases hit the wire: the metadata read and the cleanup itself.
            Assert.Equal(1, cluster.CountRequests(RequestCode.ControllerGetMetadataInfo));
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void CleanControllerBrokerData_LivingBrokerModeTravelsAsTrue_AndTheIdListFollows()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "clean_broker_living");
        ScriptLeader(cluster, cluster.BrokerAddrs[0]);
        try
        {
            admin.CleanControllerBrokerData(cluster.BrokerAddrs[0], "MockCluster", "broker-a",
                "1,2", isCleanLivingBroker: true);

            WireRecord rec = Single(cluster, RequestCode.CleanBrokerData);
            Assert.Equal("true", rec.Ext["isCleanLivingBroker"]);
            Assert.Equal("1,2", rec.Ext["brokerControllerIdsToClean"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void CleanControllerBrokerData_ARefusalIsABrokerException()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "clean_broker_err");
        ScriptLeader(cluster, cluster.BrokerAddrs[0]);
        cluster.SetReplyCode(RequestCode.CleanBrokerData, 16, "still alive");
        try
        {
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.CleanControllerBrokerData(cluster.BrokerAddrs[0], "MockCluster", "broker-a"));
            Assert.Equal(16, e.ResponseCode);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    /// <summary>A metadata failure aborts the two-phase call before the cleanup is ever sent:
    /// the broker must not see a cleanup frame at all.</summary>
    [Fact]
    public void CleanControllerBrokerData_WhenMetadataFailsNoCleanupFrameLeaves()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = Started(cluster, "clean_broker_meta_err");
        ScriptLeader(cluster, cluster.BrokerAddrs[0]);
        cluster.SetReplyCode(RequestCode.ControllerGetMetadataInfo, 16, "no quorum");
        try
        {
            MQBrokerException e = Assert.Throws<MQBrokerException>(() =>
                admin.CleanControllerBrokerData(cluster.BrokerAddrs[0], "MockCluster", "broker-a"));
            Assert.Equal(16, e.ResponseCode);
            Assert.Equal(0, cluster.CountRequests(RequestCode.CleanBrokerData));
        }
        finally
        {
            admin.Shutdown();
        }
    }
}
