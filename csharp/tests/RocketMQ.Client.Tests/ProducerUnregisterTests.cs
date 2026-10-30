// Offline unit tests for UNREGISTER_CLIENT(35) being sent when a producer/consumer exits
// -- no cluster needed.
//
//
// Why a fake cluster is brought along here: MockCluster's route is "one name per broker,
// master only" and a real local cluster only has one master, so the criterion "does a
// slave receive the 35 or not" simply cannot be constructed. The
// [`MasterSlaveCluster`] below deliberately hangs a master(0) plus a slave(1) under the
// same brokerName. This complements the examples' `unreg-live` (a before/after
// comparison on a real machine, 204): the real machine proves "the broker really removed
// it", while this proves "what shape went on the wire and which machines it reached".
using System.Buffers.Binary;
using System.Globalization;
using System.Net;
using System.Net.Sockets;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
// PropertyMap is a global using alias on the src side (SortedDictionary<string,string>), so the test project has to declare it explicitly.
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

using Xunit;

namespace RocketMQ.Client.Tests;

public class ProducerUnregisterTests
{
    private const string Topic = "UnregNetUnitTopic";
    private const string Group = "PID_unreg_net_unit";
    private const string ClientId = "10.0.0.1@unreg-net-unit";

    private const int DefaultApiTimeout = 3000;

    // ---------------- A master + slave fake cluster ----------------

    /// <summary>The evidence of one request: which broker it landed on, which code, and
    /// what extFields / body look like.</summary>
    private sealed record Frame(string Addr, int Code, PropertyMap Ext, byte[] Body);

    /// <summary>
    /// A fake cluster speaking only the remoting protocol: 1 namesrv plus two really
    /// listening brokers under the same brokerName, a master(0) and a slave(1). The
    /// route is assembled by hand from <see cref="TopicRoute"/>, because the criterion
    /// "the unregister must reach the slave" is unreachable both on a real machine and
    /// with MockCluster.
    /// </summary>
    private sealed class MasterSlaveCluster : IDisposable
    {
        private const int MaxFrame = 20 * 1024 * 1024;

        private readonly object _gate = new();
        private readonly List<Socket> _listeners = new();
        private readonly List<Frame> _frames = new();
        private readonly Dictionary<string, int> _failByAddr = new();

        public string NamesrvAddr { get; private init; } = string.Empty;
        public string MasterAddr { get; private init; } = string.Empty;
        public string SlaveAddr { get; private init; } = string.Empty;
        public string BrokerName { get; } = "broker-ms";

        public static MasterSlaveCluster Start()
        {
            Socket namesrv = BindLoopback();
            Socket master = BindLoopback();
            Socket slave = BindLoopback();
            var cluster = new MasterSlaveCluster
            {
                NamesrvAddr = EndPointOf(namesrv),
                MasterAddr = EndPointOf(master),
                SlaveAddr = EndPointOf(slave),
            };
            cluster._listeners.AddRange(new[] { namesrv, master, slave });
            Serve(namesrv, cluster.NamesrvRespond);
            Serve(master, req => cluster.BrokerRespond(cluster.MasterAddr, req));
            Serve(slave, req => cluster.BrokerRespond(cluster.SlaveAddr, req));
            return cluster;
        }

        private TopicRouteData TopicRoute()
        {
            var route = new TopicRouteData();
            route.QueueDatas.Add(new QueueData(BrokerName, 2, 2,
                PermName.PermRead | PermName.PermWrite, 0));
            route.BrokerDatas.Add(new BrokerData("MsCluster", BrokerName,
                new SortedDictionary<long, string> { { 0L, MasterAddr }, { 1L, SlaveAddr } }));
            return route;
        }

        /// <summary>Makes a broker reply to (any) request with the given response code from
        /// now on, used to construct a single-machine failure.</summary>
        public void FailFrom(string addr, int responseCode)
        {
            lock (_gate)
            {
                _failByAddr[addr] = responseCode;
            }
        }

        public List<Frame> Frames(int code)
        {
            lock (_gate)
            {
                return _frames.Where(f => f.Code == code).ToList();
            }
        }

        public int CountAt(string addr, int code) => Frames(code).Count(f => f.Addr == addr);

        public void ClearFrames()
        {
            lock (_gate)
            {
                _frames.Clear();
            }
        }

        private RemotingCommand? NamesrvRespond(RemotingCommand req)
        {
            Record(NamesrvAddr, req);
            if (req.Code == RequestCode.GetRouteinfoByTopic)
            {
                RemotingCommand resp = Respond(req, ResponseCode.Success, null);
                byte[] body = TopicRoute().Encode();
                resp.Body = body;
                resp.HasBody = true;
                return resp;
            }

            return Respond(req, ResponseCode.Success, null);
        }

        private RemotingCommand? BrokerRespond(string addr, RemotingCommand req)
        {
            Record(addr, req);
            int code;
            lock (_gate)
            {
                code = _failByAddr.TryGetValue(addr, out int scripted)
                    ? scripted
                    : ResponseCode.Success;
            }

            return req.IsOnewayRpc()
                ? null
                : Respond(req, code, code == ResponseCode.Success ? null : "mock failure");
        }

        private void Record(string addr, RemotingCommand req)
        {
            lock (_gate)
            {
                _frames.Add(new Frame(addr, req.Code, new PropertyMap(req.ExtFields),
                    req.Body ?? Array.Empty<byte>()));
            }
        }

        public void Dispose()
        {
            foreach (Socket s in _listeners)
            {
                try
                {
                    s.Dispose();
                }
                catch (Exception)
                {
                    // Closing the listening port does not need any exception handling
                }
            }
        }
    }

    // ---------------- Frame send/receive (the same minimal implementation as MockCluster) ----------------

    private static RemotingCommand Respond(RemotingCommand req, int code, string? remark)
    {
        RemotingCommand resp = RemotingCommand.CreateResponseCommand(code, remark);
        resp.Opaque = req.Opaque;
        resp.SerializeTypeCurrentRpc = req.SerializeTypeCurrentRpc;
        return resp;
    }

    private static void Serve(Socket listener, Func<RemotingCommand, RemotingCommand?> respond)
    {
        new Thread(() =>
        {
            while (true)
            {
                Socket client;
                try
                {
                    client = listener.Accept();
                }
                catch (Exception)
                {
                    return; // after Dispose, Accept necessarily throws, this is a normal shutdown
                }

                new Thread(() =>
                {
                    using (client)
                    {
                        try
                        {
                            ServeFrames(client, respond);
                        }
                        catch (Exception)
                        {
                            // A mid-stream disconnect or a decode failure: just drop this connection
                        }
                    }
                })
                { IsBackground = true }.Start();
            }
        })
        { IsBackground = true }.Start();
    }

    private static void ServeFrames(Socket client, Func<RemotingCommand, RemotingCommand?> respond)
    {
        var stream = new NetworkStream(client, ownsSocket: false);
        var lenBuf = new byte[4];
        while (true)
        {
            if (!ReadFully(stream, lenBuf, 0, 4))
            {
                return;
            }

            int total = BinaryPrimitives.ReadInt32BigEndian(lenBuf);
            if (total <= 0 || total > MaxFrameLocal)
            {
                return;
            }

            var frame = new byte[4 + total];
            Buffer.BlockCopy(lenBuf, 0, frame, 0, 4);
            if (!ReadFully(stream, frame, 4, total))
            {
                return;
            }

            if (!RemotingCommand.TryDecode(frame, out RemotingCommand req, out _))
            {
                return;
            }

            RemotingCommand? resp = respond(req);
            if (resp is null)
            {
                continue;
            }

            byte[] wire = resp.Encode();
            stream.Write(wire, 0, wire.Length);
        }
    }

    private const int MaxFrameLocal = 20 * 1024 * 1024;

    private static bool ReadFully(Stream stream, byte[] buffer, int offset, int count)
    {
        int read = 0;
        while (read < count)
        {
            int n;
            try
            {
                n = stream.Read(buffer, offset + read, count - read);
            }
            catch (Exception)
            {
                return false;
            }

            if (n <= 0)
            {
                return false;
            }

            read += n;
        }

        return true;
    }

    private static Socket BindLoopback()
    {
        var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        server.Listen(16);
        return server;
    }

    private static string EndPointOf(Socket server) =>
        ((IPEndPoint)server.LocalEndPoint!).ToString();

    /// <summary>An instance pointing at the fake cluster with the route already cached
    /// (the connections are closed on the way out).</summary>
    private static MQClientInstance StartedInstance(MasterSlaveCluster cluster)
    {
        var inst = new MQClientInstance(ClientId, new[] { cluster.NamesrvAddr });
        Assert.NotNull(inst.GetTopicRouteData(Topic));
        return inst;
    }

    // ---------------- 1~3: the shape of the header ----------------
    [Fact]
    public void ProducerSideCarriesOnlyClientIdAndProducerGroup()
    {
        using MasterSlaveCluster cluster = MasterSlaveCluster.Start();
        using MQClientInstance inst = StartedInstance(cluster);

        inst.UnregisterClient(cluster.MasterAddr, ClientId, Group, "", DefaultApiTimeout);

        List<Frame> got = cluster.Frames(RequestCode.UnregisterClient);
        Assert.Single(got);
        PropertyMap ext = got[0].Ext;
        Assert.Equal(ClientId, ext["clientID"]);
        Assert.Equal(Group, ext["producerGroup"]);
        Assert.False(ext.ContainsKey("consumerGroup"));
        Assert.Equal(2, ext.Count);
    }

    [Fact]
    public void ConsumerSideCarriesOnlyClientIdAndConsumerGroup()
    {
        using MasterSlaveCluster cluster = MasterSlaveCluster.Start();
        using MQClientInstance inst = StartedInstance(cluster);

        inst.UnregisterClient(cluster.MasterAddr, ClientId, "", Group, DefaultApiTimeout);

        PropertyMap ext = Assert.Single(cluster.Frames(RequestCode.UnregisterClient)).Ext;
        Assert.Equal(ClientId, ext["clientID"]);
        Assert.Equal(Group, ext["consumerGroup"]);
        Assert.False(ext.ContainsKey("producerGroup"));
        Assert.Equal(2, ext.Count);
    }

    [Fact]
    public void BothSidesPresentWhenBothGiven()
    {
        using MasterSlaveCluster cluster = MasterSlaveCluster.Start();
        using MQClientInstance inst = StartedInstance(cluster);

        inst.UnregisterClient(cluster.MasterAddr, ClientId, "PID_a", "GID_b", DefaultApiTimeout);

        PropertyMap ext = Assert.Single(cluster.Frames(RequestCode.UnregisterClient)).Ext;
        // PropertyMap is a SortedDictionary => what is locked in here is "all three keys are present", ordered lexicographically
        Assert.Equal(3, ext.Count);
        Assert.Equal("PID_a", ext["producerGroup"]);
        Assert.Equal("GID_b", ext["consumerGroup"]);
    }

    [Fact]
    public void WhitespaceGroupIsTreatedAsAbsent()
    {
        using MasterSlaveCluster cluster = MasterSlaveCluster.Start();
        using MQClientInstance inst = StartedInstance(cluster);

        // Pure whitespace is treated like an empty string: a legal group name can never be
        // all whitespace (Validators would have rejected it), so getting one here can only
        // mean the caller left the value out. The broker checks `group != null`, so it must
        // not go on the wire at all.
        inst.UnregisterClient(cluster.MasterAddr, ClientId, "   ", Group, DefaultApiTimeout);

        PropertyMap ext = Assert.Single(cluster.Frames(RequestCode.UnregisterClient)).Ext;
        Assert.False(ext.ContainsKey("producerGroup"));
        Assert.Equal(Group, ext["consumerGroup"]);
        Assert.Equal(2, ext.Count);
    }

    // ---------------- 4: the fan-out includes the slave ----------------

    [Fact]
    public void FanOutReachesSlaveToo_WhileHeartbeatHelperStaysMasterOnly()
    {
        using MasterSlaveCluster cluster = MasterSlaveCluster.Start();
        using MQClientInstance inst = StartedInstance(cluster);

        // Division of labour: the paths that are "a heartbeat / ask one machine and be done" only hit the preferred master...
        Assert.Equal(new[] { cluster.MasterAddr }, inst.GetRouteOfAllBrokers());
        Assert.Equal(new[] { cluster.MasterAddr, cluster.SlaveAddr }.OrderBy(a => a, StringComparer.Ordinal)
                     .ToList(),
            inst.GetAllBrokerAddrs().OrderBy(a => a, StringComparer.Ordinal).ToList());

        inst.UnregisterClientAllBrokers(ClientId, Group, "", DefaultApiTimeout);

        List<Frame> unregs = cluster.Frames(RequestCode.UnregisterClient);
        Assert.Equal(2, unregs.Count);
        Assert.Equal(1, cluster.CountAt(cluster.MasterAddr, RequestCode.UnregisterClient));
        Assert.Equal(1, cluster.CountAt(cluster.SlaveAddr, RequestCode.UnregisterClient));
        foreach (Frame f in unregs)
        {
            Assert.Equal(ClientId, f.Ext["clientID"]);
            Assert.Equal(Group, f.Ext["producerGroup"]);
            Assert.False(f.Ext.ContainsKey("consumerGroup"));
        }
    }

    // ---------------- 4b: the consumer heartbeat also covers the slave ----------------
    [Fact]
    public void ConsumerHeartbeatReachesSlaveToo()
    {
        using MasterSlaveCluster cluster = MasterSlaveCluster.Start();
        cluster.ClearFrames();

        var consumer = new DefaultMQPushConsumer(Group);
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.Subscribe(Topic, "*");
        consumer.SetMessageListener(new NoopListener());
        consumer.Start();
        try
        {
            // At least once per broker: opening the connection during startup also triggers the
            // channel-active re-registration beat, so a start can beat a broker more than once.
            // The subject here is that the slave is included at all -- a master-only fan-out
            // necessarily scores 0 on the slave.
            Assert.True(cluster.CountAt(cluster.MasterAddr, RequestCode.HeartBeat) >= 1);
            Assert.True(cluster.CountAt(cluster.SlaveAddr, RequestCode.HeartBeat) >= 1);
            Frame onSlave = cluster.Frames(RequestCode.HeartBeat).Last(f => f.Addr == cluster.SlaveAddr);
            // What the slave gets must be the report **carrying the consumer group** (it builds its own ConsumerGroupInfo from that)
            Assert.True(HeartbeatData.Decode(onSlave.Body, out HeartbeatData hb));
            Assert.Contains(Group, hb.ConsumerDataSet.Select(cd => cd.GroupName));
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    /// <summary>A listener that does nothing: this case only cares about the few heartbeats
    /// that start() sends.</summary>
    private sealed class NoopListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context) => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }

    [Fact]
    public void OneBrokerFailingDoesNotAbortTheFanOut()    {
        using MasterSlaveCluster cluster = MasterSlaveCluster.Start();
        using MQClientInstance inst = StartedInstance(cluster);
        cluster.FailFrom(cluster.SlaveAddr, ResponseCode.SystemError);

        // The lower-level one **must** throw (carrying the broker's response code), otherwise the "swallow" below would degenerate into "nothing was sent"
        MQBrokerException ex = Assert.Throws<MQBrokerException>(() =>
            inst.UnregisterClient(cluster.SlaveAddr, ClientId, Group, "", DefaultApiTimeout));
        Assert.Equal(ResponseCode.SystemError, ex.ResponseCode);

        cluster.ClearFrames();
        inst.UnregisterClientAllBrokers(ClientId, Group, "", DefaultApiTimeout);
        Assert.Equal(1, cluster.CountAt(cluster.MasterAddr, RequestCode.UnregisterClient));
        Assert.Equal(1, cluster.CountAt(cluster.SlaveAddr, RequestCode.UnregisterClient));
    }


    [Fact]
    public void UnregisterTimeoutMatchesLegacyApiTimeout()
    {
        Assert.Equal(3000, MQClientInstance.MqClientApiTimeoutMillis);
    }

    // ---------------- 6: a producer's shutdown really sends this one, and it comes after the send ----------------

    [Fact]
    public void ProducerShutdownUnregistersOnEveryKnownBrokerAfterSending()
    {
        using MockCluster cluster = MockCluster.Start(2);
        var p = new DefaultMQProducer(Group + "_live") { NamesrvAddr = cluster.NamesrvAddr };
        p.Start();
        SendResult r = p.Send(new Message(Topic, System.Text.Encoding.UTF8.GetBytes("hi")), 5000);
        Assert.Equal(SendStatus.SendOk, r.SendStatus);
        Assert.Equal(0, cluster.CountRequests(RequestCode.UnregisterClient));

        p.Shutdown();

        // One each to the two brokers (each is its own master in the route)
        Assert.Equal(2, cluster.CountRequests(RequestCode.UnregisterClient));
        Assert.Equal(2, cluster.CountRequestsWith(RequestCode.UnregisterClient,
            "producerGroup", p.ProducerGroup));
        Assert.False(cluster.AnyRequestHas(RequestCode.UnregisterClient, "consumerGroup"));

        // The 35 must come after the business send: it goes over the **long connection that is not closed yet**.
        List<WireRecord> records = cluster.Records();
        int lastSend = records.FindLastIndex(x =>
            x.Code is RequestCode.SendMessage or RequestCode.SendMessageV2);
        int firstUnreg = records.FindIndex(x => x.Code == RequestCode.UnregisterClient);
        Assert.True(lastSend >= 0 && firstUnreg > lastSend,
            "last_send=" + lastSend.ToString(CultureInfo.InvariantCulture)
            + " first_unreg=" + firstUnreg.ToString(CultureInfo.InvariantCulture)
            + " records=" + records.Count.ToString(CultureInfo.InvariantCulture));
        // What is unregistered is **this clientId** (not some other group passing itself off), and the value must be exactly what the producer computed
        Assert.Equal(p.ClientId, records[firstUnreg].Ext.GetValueOrDefault("clientID"));
    }

    [Fact]
    public void DeadBrokerInRouteDoesNotBreakProducerShutdown()
    {
        using MockCluster cluster = MockCluster.StartAt(
            new List<string?> { null, MockCluster.DeadAddr() });
        var p = new DefaultMQProducer(Group + "_dead") { NamesrvAddr = cluster.NamesrvAddr };
        p.Start();
        // Only send to the live broker-0, so the send itself does not run into a dead address
        p.Send(new Message(Topic, System.Text.Encoding.UTF8.GetBytes("hi")),
            new MessageQueue(Topic, MockCluster.BrokerName(0), 0), 5000);

        p.Shutdown();

        Assert.Equal(1, cluster.CountRequestsWith(RequestCode.UnregisterClient,
            "producerGroup", p.ProducerGroup));
    }

}
