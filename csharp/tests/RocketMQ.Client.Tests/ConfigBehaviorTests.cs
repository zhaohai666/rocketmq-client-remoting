// Focused coverage for the client-config switches that only show up on the wire:
// body materialisation during pull/POP decode, the internal API timeout, the concurrent
// heartbeat round, and the metadata page size + paging sequence.
using System.Diagnostics;
using System.Globalization;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class ConfigBehaviorTests
{
    private const string Topic = "CfgGapsTopic";
    private const string Group = "GID_cfg_gaps";
    private const string Broker = "broker-0";

    private static MessageQueue Queue0() => new(Topic, Broker, 0);

    private static Dictionary<string, string> PullResp(long next) => new()
    {
        ["nextBeginOffset"] = next.ToString(CultureInfo.InvariantCulture),
        ["minOffset"] = "0",
        ["maxOffset"] = next.ToString(CultureInfo.InvariantCulture),
    };

    private static MessageExt Message(long queueOffset, string body)
    {
        var m = new MessageExt
        {
            Topic = Topic,
            QueueId = 0,
            QueueOffset = queueOffset,
            SysFlag = 0,
            BornTimestamp = 1700000000000L,
            StoreTimestamp = 1700000000001L,
            BornHost = "127.0.0.1",
            BornHostPort = 10911,
            StoreHost = "127.0.0.1",
            StoreHostPort = 10911,
            CommitLogOffset = 4096 + queueOffset,
            Body = Encoding.UTF8.GetBytes(body),
            Properties = new PropertyMap { ["k"] = "v" + queueOffset.ToString(CultureInfo.InvariantCulture) },
        };
        return m;
    }

    private static byte[] TwoMessageBody()
    {
        byte[] first = MessageDecoder.EncodeMessageExt(Message(10, "first-body"), false);
        byte[] second = MessageDecoder.EncodeMessageExt(Message(11, "second-body"), false);
        var all = new byte[first.Length + second.Length];
        Buffer.BlockCopy(first, 0, all, 0, first.Length);
        Buffer.BlockCopy(second, 0, all, first.Length, second.Length);
        return all;
    }

    private static MQClientInstance NewInstance(string id) =>
        new(id, new List<string> { "127.0.0.1:1" });

    private static HeartbeatData ProducerHeartbeat() => new("cfg-client")
    {
        ProducerDataSet = { new ProducerData("PG_cfg") },
    };

    private static JsonValue Version(int counter)
    {
        JsonValue v = JsonValue.MakeObject();
        v.Set("counter", JsonValue.MakeInt(counter));
        v.Set("timestamp", JsonValue.MakeInt(1700000000000L + counter));
        return v;
    }

    private static byte[] TopicPage(JsonValue version, params string[] names)
    {
        var w = new TopicConfigSerializeWrapper { DataVersion = version };
        foreach (string name in names)
        {
            w.TopicConfigTable[name] = new TopicConfig(name);
        }

        return w.Encode();
    }

    private static byte[] GroupPage(JsonValue version, params string[] names)
    {
        var w = new SubscriptionGroupWrapper { DataVersion = version };
        foreach (string name in names)
        {
            w.SubscriptionGroupTable[name] = new SubscriptionGroupConfig { GroupName = name };
        }

        return w.Encode();
    }

    private static DefaultMQAdminExt StartedAdmin(MockCluster cluster, string tag,
        int? pageSize = null)
    {
        var admin = new DefaultMQAdminExt("ADMIN_cfg_" + tag);
        admin.SetNamesrvAddr(cluster.NamesrvAddr);
        if (pageSize is not null)
        {
            admin.MaxPageSizeInGetMetadata = pageSize.Value;
        }

        admin.Start();
        return admin;
    }

    private static WireRecord SingleRequest(MockCluster cluster, int code)
    {
        List<WireRecord> records = cluster.Records().Where(r => r.Code == code).ToList();
        Assert.Single(records);
        return records[0];
    }

    // ---------------------------------------------------------------- defaults

    [Fact]
    public void ClientConfigDefaults_AreTheDocumentedValues()
    {
        MQClientInstance instance = NewInstance("cfg_defaults_instance");
        try
        {
            Assert.True(instance.DecodeReadBody);
            Assert.Equal(3000, instance.MqClientApiTimeout);
            Assert.False(instance.EnableConcurrentHeartbeat);
            Assert.Equal(Environment.ProcessorCount, instance.ConcurrentHeartbeatThreadPoolSize);
            Assert.Equal(2000, instance.MaxPageSizeInGetMetadata);
        }
        finally
        {
            instance.Shutdown();
        }

        var producer = new DefaultMQProducer("PG_cfg_defaults");
        Assert.True(producer.DecodeReadBody);
        Assert.Equal(3000, producer.MqClientApiTimeout);
        Assert.False(producer.EnableConcurrentHeartbeat);
        Assert.Equal(Environment.ProcessorCount, producer.ConcurrentHeartbeatThreadPoolSize);
        Assert.Equal(2000, producer.MaxPageSizeInGetMetadata);

        var push = new DefaultMQPushConsumer("CID_cfg_defaults");
        Assert.True(push.DecodeReadBody);
        Assert.Equal(3000, push.MqClientApiTimeout);
        Assert.False(push.EnableConcurrentHeartbeat);
        Assert.Equal(Environment.ProcessorCount, push.ConcurrentHeartbeatThreadPoolSize);
        Assert.Equal(2000, push.MaxPageSizeInGetMetadata);

        var pull = new DefaultMQPullConsumer("PG_cfg_defaults_pull");
        Assert.True(pull.DecodeReadBody);
        Assert.Equal(3000, pull.MqClientApiTimeout);
        Assert.False(pull.EnableConcurrentHeartbeat);
        Assert.Equal(Environment.ProcessorCount, pull.ConcurrentHeartbeatThreadPoolSize);
        Assert.Equal(2000, pull.MaxPageSizeInGetMetadata);

        var lite = new DefaultLitePullConsumer("PG_cfg_defaults_lite");
        Assert.True(lite.DecodeReadBody);
        Assert.Equal(3000, lite.MqClientApiTimeout);
        Assert.False(lite.EnableConcurrentHeartbeat);
        Assert.Equal(Environment.ProcessorCount, lite.ConcurrentHeartbeatThreadPoolSize);
        Assert.Equal(2000, lite.MaxPageSizeInGetMetadata);

        var admin = new DefaultMQAdminExt("ADMIN_cfg_defaults");
        Assert.Equal(2000, admin.MaxPageSizeInGetMetadata);

        // The setters are not read-only decorations.
        producer.DecodeReadBody = false;
        producer.MqClientApiTimeout = 4321;
        producer.EnableConcurrentHeartbeat = true;
        producer.ConcurrentHeartbeatThreadPoolSize = 3;
        producer.MaxPageSizeInGetMetadata = 77;
        Assert.False(producer.DecodeReadBody);
        Assert.Equal(4321, producer.MqClientApiTimeout);
        Assert.True(producer.EnableConcurrentHeartbeat);
        Assert.Equal(3, producer.ConcurrentHeartbeatThreadPoolSize);
        Assert.Equal(77, producer.MaxPageSizeInGetMetadata);

        push.DecodeReadBody = false;
        pull.DecodeReadBody = false;
        lite.DecodeReadBody = false;
        Assert.False(push.DecodeReadBody);
        Assert.False(pull.DecodeReadBody);
        Assert.False(lite.DecodeReadBody);

        admin.MaxPageSizeInGetMetadata = 55;
        Assert.Equal(55, admin.MaxPageSizeInGetMetadata);

        // A non-positive value falls back to the documented default instead of poisoning
        // the page size or the worker-pool width.
        producer.MqClientApiTimeout = 0;
        producer.ConcurrentHeartbeatThreadPoolSize = 0;
        producer.MaxPageSizeInGetMetadata = 0;
        Assert.Equal(3000, producer.MqClientApiTimeout);
        Assert.Equal(Environment.ProcessorCount, producer.ConcurrentHeartbeatThreadPoolSize);
        Assert.Equal(2000, producer.MaxPageSizeInGetMetadata);
    }

    [Fact]
    public void FacadeConfig_ReachesTheSharedInstance()
    {
        using MockCluster cluster = MockCluster.Start(1);
        var producer = new DefaultMQProducer("PG_cfg_forward");
        producer.InstanceName = "cfg_forward_producer";
        producer.NamesrvAddr = cluster.NamesrvAddr;
        producer.DecodeReadBody = false;
        producer.MqClientApiTimeout = 4321;
        producer.EnableConcurrentHeartbeat = true;
        producer.ConcurrentHeartbeatThreadPoolSize = 3;
        producer.MaxPageSizeInGetMetadata = 77;
        producer.Start();
        try
        {
            MQClientInstance client = producer.Client();
            Assert.False(client.DecodeReadBody);
            Assert.Equal(4321, client.MqClientApiTimeout);
            Assert.True(client.EnableConcurrentHeartbeat);
            Assert.Equal(3, client.ConcurrentHeartbeatThreadPoolSize);
            Assert.Equal(77, client.MaxPageSizeInGetMetadata);
        }
        finally
        {
            producer.Shutdown();
        }

        var push = new DefaultMQPushConsumer("CID_cfg_forward");
        push.InstanceName = "cfg_forward_consumer";
        push.SetNamesrvAddr(cluster.NamesrvAddr);
        push.Subscribe(Topic, "*");
        push.SetMessageListener(new NullListener());
        push.DecodeReadBody = false;
        push.MqClientApiTimeout = 4321;
        push.EnableConcurrentHeartbeat = true;
        push.ConcurrentHeartbeatThreadPoolSize = 3;
        push.MaxPageSizeInGetMetadata = 77;
        push.Start();
        try
        {
            MQClientInstance client = push.Client();
            Assert.False(client.DecodeReadBody);
            Assert.Equal(4321, client.MqClientApiTimeout);
            Assert.True(client.EnableConcurrentHeartbeat);
            Assert.Equal(3, client.ConcurrentHeartbeatThreadPoolSize);
            Assert.Equal(77, client.MaxPageSizeInGetMetadata);
        }
        finally
        {
            push.Shutdown();
        }
    }

    private sealed class NullListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context) => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }

    // ---------------------------------------------------------------- decode switch

    [Fact]
    public void DecodeReadBody_DefaultPullReturnsBodies_OffReturnsHeaderOnlyAndAdvancesCursor()
    {
        using MockCluster cluster = MockCluster.Start(1);
        MQClientInstance instance = NewInstance("cfg_decode_instance");
        try
        {
            // Control: the default really materialises both frames.
            cluster.ScriptPull(ResponseCode.Success, PullResp(12), TwoMessageBody());
            PullResult withBodies = instance.PullMessage(Group, Queue0(), 10, 32, 0, 0,
                "*", 0, "TAG", addrIn: cluster.BrokerAddrs[0]);
            Assert.True(withBodies.IsFound);
            Assert.Equal(2, withBodies.MsgFoundList.Count);
            Assert.Equal("first-body", Encoding.UTF8.GetString(withBodies.MsgFoundList[0].Body));
            Assert.Equal("second-body", Encoding.UTF8.GetString(withBodies.MsgFoundList[1].Body));
            Assert.True(withBodies.MsgFoundList[0].HasBody);

            // Off: the same two frames come back, the cursor walks past both, but bodies
            // are empty and the headers (offset / properties) survive.
            instance.DecodeReadBody = false;
            cluster.ScriptPull(ResponseCode.Success, PullResp(22), TwoMessageBody());
            PullResult headerOnly = instance.PullMessage(Group, Queue0(), 20, 32, 0, 0,
                "*", 0, "TAG", addrIn: cluster.BrokerAddrs[0]);
            Assert.True(headerOnly.IsFound);
            Assert.Equal(2, headerOnly.MsgFoundList.Count);
            Assert.Equal(new long[] { 10, 11 }, headerOnly.MsgFoundList.Select(m => m.QueueOffset));
            Assert.All(headerOnly.MsgFoundList, m => Assert.Empty(m.Body));
            Assert.All(headerOnly.MsgFoundList, m => Assert.False(m.HasBody));
            Assert.Equal("v10", headerOnly.MsgFoundList[0].Properties["k"]);
            Assert.Equal("v11", headerOnly.MsgFoundList[1].Properties["k"]);

            // Back on: the switch is read at call time, not captured at construction.
            instance.DecodeReadBody = true;
            cluster.ScriptPull(ResponseCode.Success, PullResp(32), TwoMessageBody());
            PullResult again = instance.PullMessage(Group, Queue0(), 30, 32, 0, 0,
                "*", 0, "TAG", addrIn: cluster.BrokerAddrs[0]);
            Assert.Equal("first-body", Encoding.UTF8.GetString(again.MsgFoundList[0].Body));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    [Fact]
    public void DecodeReadBody_OffAppliesToLitePullAndPop()
    {
        using MockCluster cluster = MockCluster.Start(1);
        MQClientInstance instance = NewInstance("cfg_decode_lite_pop");
        try
        {
            instance.DecodeReadBody = false;
            int liteFlag = PullSysFlag.BuildSysFlag(false, false, false, false, litePull: true);

            // The lite-pull request flag path must obey the same switch.
            cluster.ScriptPull(ResponseCode.Success, PullResp(2), TwoMessageBody());
            PullResult lite = instance.PullMessage(Group, Queue0(), 0, 32, liteFlag, 0,
                "*", 0, "TAG", addrIn: cluster.BrokerAddrs[0]);
            Assert.Equal(1, cluster.CountRequests(RequestCode.LitePullMessage));
            Assert.Equal(2, lite.MsgFoundList.Count);
            Assert.All(lite.MsgFoundList, m => Assert.Empty(m.Body));
            Assert.All(lite.MsgFoundList, m => Assert.False(m.HasBody));

            // POP uses the same batch decode path, then stamps POP_CK on the header-only
            // message -- that stamp must still be present.
            cluster.SetReplyBody(RequestCode.PopMessage, _ => TwoMessageBody());
            PopResult pop = instance.PopMessage(Group, Topic, 0, 32, 30000L, 1000L, 0,
                brokerNameIn: Broker, addrIn: cluster.BrokerAddrs[0]);
            Assert.True(pop.Status == PopStatus.Found);
            Assert.Equal(2, pop.MsgFoundList.Count);
            Assert.All(pop.MsgFoundList, m => Assert.Empty(m.Body));
            Assert.All(pop.MsgFoundList, m => Assert.False(m.HasBody));
            Assert.All(pop.MsgFoundList, m =>
                Assert.True(m.Properties.ContainsKey(MessageConst.PropertyPopCk)));

            // Control: with the switch on the same POP body really carries bytes.
            instance.DecodeReadBody = true;
            cluster.SetReplyBody(RequestCode.PopMessage, _ => TwoMessageBody());
            PopResult popWithBody = instance.PopMessage(Group, Topic, 0, 32, 30000L, 1000L, 0,
                brokerNameIn: Broker, addrIn: cluster.BrokerAddrs[0]);
            Assert.Equal("first-body", Encoding.UTF8.GetString(popWithBody.MsgFoundList[0].Body));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    // ---------------------------------------------------------------- internal API timeout

    [Fact]
    public void MqClientApiTimeout_Default3000ReachesTheHeartbeatAndAnExplicitValueWins()
    {
        using MockCluster cluster = MockCluster.Start(1);
        string addr = cluster.BrokerAddrs[0];
        cluster.MakeSilent(0);
        MQClientInstance instance = NewInstance("cfg_timeout_instance");
        try
        {
            var sw = Stopwatch.StartNew();
            RemotingTimeoutException e = Assert.Throws<RemotingTimeoutException>(
                () => instance.SendHeartbeat(addr, ProducerHeartbeat()));
            sw.Stop();

            // The request really went on the wire; the default timeout is the 3000ms
            // carried into the transport wait.
            Assert.Equal(1, cluster.CountRequests(RequestCode.HeartBeat));
            Assert.Contains("3000 ms", e.Message);
            Assert.InRange(sw.ElapsedMilliseconds, 2500L, 8000L);

            // A configured value applies to the default path...
            instance.MqClientApiTimeout = 5000;
            cluster.MakeSilent(0);
            sw.Restart();
            RemotingTimeoutException configured = Assert.Throws<RemotingTimeoutException>(
                () => instance.SendHeartbeat(addr, ProducerHeartbeat()));
            sw.Stop();
            Assert.Contains("5000 ms", configured.Message);

            // ...and an explicit per-call value still wins over it.
            cluster.MakeSilent(0);
            sw.Restart();
            RemotingTimeoutException explicitTimeout = Assert.Throws<RemotingTimeoutException>(
                () => instance.SendHeartbeat(addr, ProducerHeartbeat(), 700));
            sw.Stop();
            Assert.Contains("700 ms", explicitTimeout.Message);
            Assert.InRange(sw.ElapsedMilliseconds, 400L, 3000L);
            Assert.Equal(3, cluster.CountRequests(RequestCode.HeartBeat));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    // ---------------------------------------------------------------- concurrent heartbeat

    private static byte[] TwoBrokerRoute(MockCluster cluster)
    {
        IReadOnlyList<string> addrs = cluster.BrokerAddrs;
        var route = new TopicRouteData();
        route.QueueDatas.Add(new QueueData("broker-0", 1, 1,
            PermName.PermRead | PermName.PermWrite, 0));
        route.BrokerDatas.Add(new BrokerData("cfg-cluster", "broker-0",
            new SortedDictionary<long, string>
            {
                { MixAll.MasterId, addrs[0] },
                { 1L, addrs[1] },
            }));
        route.QueueDatas.Add(new QueueData("broker-1", 1, 1,
            PermName.PermRead | PermName.PermWrite, 0));
        route.BrokerDatas.Add(new BrokerData("cfg-cluster", "broker-1",
            new SortedDictionary<long, string>
            {
                { MixAll.MasterId, addrs[2] },
            }));
        return route.Encode();
    }

    [Fact]
    public async Task ConcurrentHeartbeat_HitsEveryMasterWhileSerialOnlyReachesOneAtATime()
    {
        using MockCluster cluster = MockCluster.Start(3);
        cluster.SetRouteBody(_ => TwoBrokerRoute(cluster));
        MQClientInstance instance = NewClientWithRoute(cluster, "cfg_hb_concurrent");
        instance.MqClientApiTimeout = 10000;
        instance.ConcurrentHeartbeatThreadPoolSize = 2;
        try
        {
            var gate = new ManualResetEventSlim(false);
            cluster.SetReplyBody(RequestCode.HeartBeat, _ =>
            {
                gate.Wait();
                return Array.Empty<byte>();
            });

            instance.EnableConcurrentHeartbeat = true;
            Task<int> round = Task.Run(() =>
                instance.SendHeartbeatToAllBrokers(ProducerHeartbeat(), includeSlaves: true));

            // With a pool of 2 both masters must be in flight behind the same gate; a
            // serial round would leave exactly one arrived. The slave (broker-0 id 1)
            // is skipped because the body has no consumer data.
            Assert.True(SpinWait.SpinUntil(
                () => cluster.RequestsAt(0, RequestCode.HeartBeat) >= 1
                      && cluster.RequestsAt(2, RequestCode.HeartBeat) >= 1,
                5000), "both masters must receive a heartbeat while the responses are gated");
            Assert.Equal(0, cluster.RequestsAt(1, RequestCode.HeartBeat));
            Assert.Equal(2, cluster.CountRequests(RequestCode.HeartBeat));

            gate.Set();
            int delivered = await round.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(2, delivered);
            Assert.Equal(1, cluster.RequestsAt(0, RequestCode.HeartBeat));
            Assert.Equal(1, cluster.RequestsAt(2, RequestCode.HeartBeat));
            Assert.Equal(0, cluster.RequestsAt(1, RequestCode.HeartBeat));

            // Paired control: with concurrency off the same gated round can only have
            // one request in flight, so the second master is not reached until release.
            var serialGate = new ManualResetEventSlim(false);
            cluster.SetReplyBody(RequestCode.HeartBeat, _ =>
            {
                serialGate.Wait();
                return Array.Empty<byte>();
            });
            instance.EnableConcurrentHeartbeat = false;
            int before = cluster.CountRequests(RequestCode.HeartBeat);
            Task<int> serial = Task.Run(() =>
                instance.SendHeartbeatToAllBrokers(ProducerHeartbeat(), includeSlaves: true));
            Assert.True(SpinWait.SpinUntil(
                () => cluster.CountRequests(RequestCode.HeartBeat) >= before + 1, 5000),
                "the serial round must still send the first heartbeat");
            bool secondArrived = SpinWait.SpinUntil(
                () => cluster.CountRequests(RequestCode.HeartBeat) >= before + 2, 750);
            Assert.False(secondArrived, "a serial round cannot have two requests in flight");
            serialGate.Set();
            int serialDelivered = await serial.WaitAsync(TimeSpan.FromSeconds(10));
            Assert.Equal(2, serialDelivered);
            Assert.Equal(before + 2, cluster.CountRequests(RequestCode.HeartBeat));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    [Fact]
    public void Heartbeat_EmptyBodySendsNothing()
    {
        using MockCluster cluster = MockCluster.Start(1);
        MQClientInstance instance = NewClientWithRoute(cluster, "cfg_hb_empty");
        try
        {
            Assert.Equal(0, instance.SendHeartbeatToAllBrokers(new HeartbeatData("cid")));
            Assert.Equal(0, cluster.CountRequests(RequestCode.HeartBeat));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    private static MQClientInstance NewClientWithRoute(MockCluster cluster, string id)
    {
        var instance = new MQClientInstance(id, new List<string> { cluster.NamesrvAddr });
        Assert.True(instance.UpdateTopicRouteInfoFromNameServer(Topic),
            "the route must enter the cache");
        return instance;
    }

    // ---------------------------------------------------------------- metadata page size

    [Fact]
    public void MaxPageSizeInGetMetadata_DefaultsTo2000OnTheWire()
    {
        using MockCluster cluster = MockCluster.Start(1);
        string addr = cluster.BrokerAddrs[0];
        cluster.SetReplyBody(RequestCode.GetAllTopicConfig, _ => TopicPage(Version(1), "T1"));
        DefaultMQAdminExt admin = StartedAdmin(cluster, "page_default");
        try
        {
            TopicConfigSerializeWrapper result = admin.GetAllTopicConfig(addr, 5000);
            WireRecord request = SingleRequest(cluster, RequestCode.GetAllTopicConfig);
            Assert.Equal("0", request.Ext["topicSeq"]);
            Assert.Equal("2000", request.Ext["maxTopicNum"]);
            // The first page sends an explicit empty dataVersion key, not an omitted one.
            Assert.True(request.Ext.TryGetValue("dataVersion", out string? dataVersion));
            Assert.Equal("", dataVersion);
            // No totalTopicNum (old broker): one page and stop.
            Assert.Equal("T1", Assert.Single(result.TopicConfigTable).Key);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void MaxPageSizeInGetMetadata_ConfiguredValueReachesTopicAndGroupPaging()
    {
        using MockCluster cluster = MockCluster.Start(1);
        string addr = cluster.BrokerAddrs[0];
        cluster.SetReplyBody(RequestCode.GetAllTopicConfig, _ => TopicPage(Version(1), "T1"));
        cluster.SetReplyBody(RequestCode.GetAllSubscriptiongroupConfig,
            _ => GroupPage(Version(1), "G1"));
        DefaultMQAdminExt admin = StartedAdmin(cluster, "page_configured", pageSize: 7);
        try
        {
            admin.GetAllTopicConfig(addr, 5000);
            admin.GetAllSubscriptionGroup(addr, 5000);

            WireRecord topicReq = SingleRequest(cluster, RequestCode.GetAllTopicConfig);
            Assert.Equal("7", topicReq.Ext["maxTopicNum"]);
            Assert.Equal("0", topicReq.Ext["topicSeq"]);
            Assert.Equal("", topicReq.Ext["dataVersion"]);

            WireRecord groupReq = SingleRequest(cluster, RequestCode.GetAllSubscriptiongroupConfig);
            Assert.Equal("7", groupReq.Ext["maxGroupNum"]);
            Assert.Equal("0", groupReq.Ext["groupSeq"]);
            Assert.True(groupReq.Ext.TryGetValue("dataVersion", out string? groupVersion));
            Assert.Equal("", groupVersion);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void GetAllTopicConfig_PagesFromEmptyVersionAndRestartsWhenTheVersionChanges()
    {
        using MockCluster cluster = MockCluster.Start(1);
        string addr = cluster.BrokerAddrs[0];
        JsonValue v1 = Version(1);
        JsonValue v2 = Version(2);

        var pageGate = new object();
        int page = 0;
        cluster.SetReplyBody(RequestCode.GetAllTopicConfig, _ =>
        {
            lock (pageGate)
            {
                page++;
                return page switch
                {
                    1 => TopicPage(v1, "A", "B"),
                    2 => TopicPage(v2, "C"),
                    3 => TopicPage(v2, "D", "E"),
                    4 => TopicPage(v2, "F", "G", "H"),
                    _ => TopicPage(v2),
                };
            }
        });
        cluster.SetReplyExt(RequestCode.GetAllTopicConfig, _ => new Dictionary<string, string>
        {
            ["totalTopicNum"] = "5",
        });

        DefaultMQAdminExt admin = StartedAdmin(cluster, "page_sequence", pageSize: 2);
        try
        {
            TopicConfigSerializeWrapper result = admin.GetAllTopicConfig(addr, 5000);

            List<WireRecord> requests = cluster.Records()
                .Where(r => r.Code == RequestCode.GetAllTopicConfig).ToList();
            Assert.Equal(4, requests.Count);
            Assert.All(requests, r => Assert.Equal("2", r.Ext["maxTopicNum"]));

            // Page 1: seq 0 with the empty-string version.
            Assert.Equal("0", requests[0].Ext["topicSeq"]);
            Assert.Equal("", requests[0].Ext["dataVersion"]);

            // Page 2: seq advanced by the number of configs the first page carried and
            // echoes the last seen version.
            Assert.Equal("2", requests[1].Ext["topicSeq"]);
            Assert.Equal(1, ParsedCounter(requests[1].Ext["dataVersion"]));

            // The broker changed the version on page 2: restart at seq 0 with the new
            // version and drop everything collected so far.
            Assert.Equal("0", requests[2].Ext["topicSeq"]);
            Assert.Equal(2, ParsedCounter(requests[2].Ext["dataVersion"]));

            // The fourth page carries the remaining three configs: seq 2 + 3 reaches the
            // total of five and the loop stops.
            Assert.Equal("2", requests[3].Ext["topicSeq"]);
            Assert.Equal(2, ParsedCounter(requests[3].Ext["dataVersion"]));

            Assert.Equal(new[] { "D", "E", "F", "G", "H" }, result.TopicConfigTable.Keys);
            Assert.Equal(2, result.DataVersion.Get("counter").IntValue());
        }
        finally
        {
            admin.Shutdown();
        }
    }

    private static int ParsedCounter(string text) =>
        (int)Json.Parse(text).Get("counter").IntValue();
}
