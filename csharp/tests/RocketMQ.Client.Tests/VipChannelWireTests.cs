// Wire-level proof of the broker's VIP-channel contract.
//
// A broker answers on the port the route advertises and, when the client opts in, on that
// port minus two. Only the landing port can prove which one a request really used, so this
// file listens on **both** ports of one broker and counts request codes per port: the
// client's own bookkeeping is not evidence.
//
// Two families are pinned:
//   - offset management and the batch queue locks move to port-2 when the switch is on and
//     stay on the route port when it is off;
//   - the requests a broker only serves on its plain port (heartbeat, unregister, pull, pop,
//     ack, invisible-time change, recall) never move, and neither does any name-server
//     request.
using System.Buffers.Binary;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class VipChannelWireTests
{
    private const string Group = "GID_VipWire";
    private const string Topic = "VipWireTopic";
    private const string BrokerName = "broker-a";
    private const string ClientId = "127.0.0.1@5678#1";
    private const int MaxFrame = 20 * 1024 * 1024;

    /// <summary>Request codes the broker serves on its VIP port as well.</summary>
    private static readonly int[] Rewritten =
    {
        RequestCode.QueryConsumerOffset,
        RequestCode.UpdateConsumerOffset,
        RequestCode.GetMaxOffset,
        RequestCode.GetMinOffset,
        RequestCode.SearchOffsetByTimestamp,
        RequestCode.LockBatchMq,
        RequestCode.UnlockBatchMq,
    };

    /// <summary>Request codes that must stay on the plain route port.</summary>
    private static readonly int[] NeverRewritten =
    {
        RequestCode.HeartBeat,
        RequestCode.UnregisterClient,
        RequestCode.PullMessage,
        RequestCode.LitePullMessage,
        RequestCode.PopMessage,
        RequestCode.AckMessage,
        RequestCode.ChangeMessageInvisibletime,
        RequestCode.RecallMessage,
    };

    private static MessageQueue Queue() => new(Topic, BrokerName, 0);

    // ---------------------------------------------------------------- harness

    /// <summary>One listening port: it answers route queries like a name server and counts
    /// every request code it receives like a broker.</summary>
    private sealed class Endpoint : IDisposable
    {
        private readonly object _gate = new();
        private readonly Dictionary<int, int> _counts = new();
        private readonly Socket _listener;
        private byte[]? _routeBody;

        public string Addr { get; }

        private Endpoint(Socket listener)
        {
            _listener = listener;
            Addr = ((IPEndPoint)listener.LocalEndPoint!).ToString();
            new Thread(AcceptLoop) { IsBackground = true }.Start();
        }

        /// <summary>Binds exactly this port, or returns null when it is already taken.</summary>
        public static Endpoint? TryBind(int port)
        {
            var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            try
            {
                server.Bind(new IPEndPoint(IPAddress.Loopback, port));
                server.Listen(16);
            }
            catch (SocketException)
            {
                // That port is already in use on this machine: the caller picks another pair
                server.Dispose();
                return null;
            }

            return new Endpoint(server);
        }

        public void SetRoute(TopicRouteData route)
        {
            lock (_gate)
            {
                _routeBody = route.Encode();
            }
        }

        public int CountFor(int code)
        {
            lock (_gate)
            {
                return _counts.TryGetValue(code, out int n) ? n : 0;
            }
        }

        public void Dispose()
        {
            try
            {
                _listener.Dispose();
            }
            catch (Exception)
            {
                // Closing the listening port does not need any exception handling
            }
        }

        private void AcceptLoop()
        {
            while (true)
            {
                Socket client;
                try
                {
                    client = _listener.Accept();
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
                            ServeFrames(client);
                        }
                        catch (Exception)
                        {
                            // A mid-stream disconnect / decode failure: just drop this connection
                        }
                    }
                })
                { IsBackground = true }.Start();
            }
        }

        private void ServeFrames(Socket client)
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
                if (total <= 0 || total > MaxFrame)
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

                RemotingCommand? resp = Respond(req);
                if (resp is null)
                {
                    continue; // a oneway request has no response to write
                }

                byte[] wire = resp.Encode();
                stream.Write(wire, 0, wire.Length);
            }
        }

        private RemotingCommand? Respond(RemotingCommand req)
        {
            byte[]? routeBody;
            lock (_gate)
            {
                _counts[req.Code] = _counts.GetValueOrDefault(req.Code) + 1;
                routeBody = _routeBody;
            }

            if (req.Code == RequestCode.GetRouteinfoByTopic)
            {
                if (routeBody is null)
                {
                    return Echo(req, ResponseCode.TopicNotExist, "mock: no route");
                }

                RemotingCommand routeResp = Echo(req, ResponseCode.Success, null);
                routeResp.Body = routeBody;
                routeResp.HasBody = true;
                return routeResp;
            }

            if (req.IsOnewayRpc())
            {
                return null;
            }

            // The answer only has to be parseable: this file asserts the landing port, not
            // what each RPC does with the response.
            RemotingCommand resp = Echo(req, ResponseCode.Success, null);
            resp.AddExtField("offset", "7");
            resp.AddExtField("minOffset", "0");
            resp.AddExtField("maxOffset", "7");
            resp.AddExtField("nextBeginOffset", "0");
            resp.AddExtField("msgId", "MOCK-RECALLED");
            if (req.Code is RequestCode.LockBatchMq or RequestCode.UnlockBatchMq)
            {
                resp.Body = Encoding.UTF8.GetBytes("{\"lockOKMQSet\":[]}");
                resp.HasBody = true;
            }

            return resp;
        }

        private static RemotingCommand Echo(RemotingCommand req, int code, string? remark)
        {
            var resp = RemotingCommand.CreateResponseCommand(code, remark);
            // The client pairs by opaque, a cross-wired response is dropped as noise
            resp.Opaque = req.Opaque;
            resp.SerializeTypeCurrentRpc = req.SerializeTypeCurrentRpc;
            return resp;
        }

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
    }

    /// <summary>One broker with both of its ports listening: the plain route port and the
    /// VIP port two below it.</summary>
    private sealed class Cluster : IDisposable
    {
        public Endpoint Plain { get; }
        public Endpoint Vip { get; }

        private Cluster(Endpoint plain, Endpoint vip)
        {
            Plain = plain;
            Vip = vip;
        }

        public static Cluster Start()
        {
            for (int attempt = 0; attempt < 24; ++attempt)
            {
                int basePort = FreePort();
                if (basePort <= 1024)
                {
                    continue;
                }

                Endpoint? plain = Endpoint.TryBind(basePort);
                if (plain is null)
                {
                    continue;
                }

                Endpoint? vip = Endpoint.TryBind(basePort - 2);
                if (vip is null)
                {
                    plain.Dispose();
                    continue;
                }

                var cluster = new Cluster(plain, vip);
                var route = new TopicRouteData();
                route.QueueDatas.Add(new QueueData(BrokerName, 1, 1,
                    PermName.PermRead | PermName.PermWrite, 0));
                // The route only ever carries the plain port: the VIP port is derived from it.
                route.BrokerDatas.Add(new BrokerData("VipWireCluster", BrokerName,
                    new SortedDictionary<long, string> { { MixAll.MasterId, plain.Addr } }));
                plain.SetRoute(route);
                vip.SetRoute(route);
                return cluster;
            }

            throw new InvalidOperationException(
                "cannot bind a loopback port pair (p, p-2) for the VIP channel cases");
        }

        public void Dispose()
        {
            Plain.Dispose();
            Vip.Dispose();
        }

        private static int FreePort()
        {
            var probe = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            int port = ((IPEndPoint)probe.LocalEndPoint!).Port;
            probe.Dispose();
            return port;
        }
    }

    /// <summary>An instance whose route points at the cluster, with the switch set before
    /// any request goes out.</summary>
    private static MQClientInstance Seeded(Cluster cluster, bool vipOn)
    {
        var instance = new MQClientInstance(ClientId, new List<string> { cluster.Plain.Addr },
            vipChannelEnabled: vipOn);
        Assert.True(instance.UpdateTopicRouteInfoFromNameServer(Topic), "the route must enter the cache");
        return instance;
    }

    /// <summary>Runs one call and ignores its outcome: whether the fake's generic answer is
    /// good enough for that RPC is not what these cases assert -- the landing port is.</summary>
    private static void Attempt(Action action)
    {
        try
        {
            action();
        }
        catch (Exception)
        {
            // Swallowed on purpose
        }
    }

    // ---------------------------------------------------------------- (i) rewritten family

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void OffsetAndBatchLocks_FollowTheSwitch(bool vipOn)
    {
        using Cluster cluster = Cluster.Start();
        MQClientInstance instance = Seeded(cluster, vipOn);
        Endpoint expected = vipOn ? cluster.Vip : cluster.Plain;
        Endpoint unused = vipOn ? cluster.Plain : cluster.Vip;
        try
        {
            MessageQueue mq = Queue();
            Assert.True(instance.QueryConsumerOffset(Group, mq, out long stored, 3000));
            Assert.Equal(7, stored);
            instance.UpdateConsumerOffset(Group, mq, 7, 3000);
            Assert.Equal(7, instance.GetMaxOffset(mq, 3000));
            Assert.Equal(7, instance.GetMinOffset(mq, 3000));
            Assert.Equal(7, instance.SearchOffsetByTimestamp(mq, 1234, 3000));
            Assert.NotNull(instance.LockBatchMq(Group, ClientId, new List<MessageQueue> { mq }, 3000));
            instance.UnlockBatchMq(Group, ClientId, new List<MessageQueue> { mq }, 3000);
        }
        finally
        {
            instance.Shutdown();
        }

        foreach (int code in Rewritten)
        {
            Assert.Equal(1, expected.CountFor(code));
            Assert.Equal(0, unused.CountFor(code));
        }
    }

    /// <summary>Turning the switch on must not move a name-server query: VIP ports exist on
    /// the broker side only, so the route fetch has to keep using the address as given.</summary>
    [Fact]
    public void NameServerRouteQuery_StaysOnTheAdvertisedPort()
    {
        using Cluster cluster = Cluster.Start();
        MQClientInstance instance = Seeded(cluster, true);
        try
        {
            Assert.True(instance.UpdateTopicRouteInfoFromNameServer(Topic));
        }
        finally
        {
            instance.Shutdown();
        }

        Assert.True(cluster.Plain.CountFor(RequestCode.GetRouteinfoByTopic) >= 2,
            "the route queries land on the name server port itself");
        Assert.Equal(0, cluster.Vip.CountFor(RequestCode.GetRouteinfoByTopic));
    }

    // ---------------------------------------------------------------- (ii) never rewritten

    [Fact]
    public void HeartbeatPullPopAckRecallAndUnregister_NeverFollowTheSwitch()
    {
        using Cluster cluster = Cluster.Start();
        MQClientInstance instance = Seeded(cluster, true);
        try
        {
            var heartbeat = new HeartbeatData
            {
                ClientId = ClientId,
                ProducerDataSet = { new ProducerData(Group) },
            };
            Assert.Equal(1, instance.SendHeartbeatToAllBrokers(heartbeat, false, 3000));

            MessageQueue mq = Queue();
            Attempt(() => instance.PullMessage(Group, mq, 0, 32,
                PullSysFlag.BuildSysFlag(true, true, true, false), 0, "*", 1L,
                RocketMQ.Common.ExpressionType.TAG, 3000));
            // The same pull asked to carry its subscription along: the lite-pull form of the
            // request must not move either.
            Attempt(() => instance.PullMessage(Group, mq, 0, 32,
                PullSysFlag.BuildSysFlag(false, true, true, false, true), 0, "*", 1L,
                RocketMQ.Common.ExpressionType.TAG, 3000, addrIn: cluster.Plain.Addr));
            Attempt(() => instance.PopMessage(Group, Topic, 0, 32, 3000, 100, 0, brokerNameIn: BrokerName,
                timeoutMillis: 3000));
            Attempt(() => instance.AckMessage(Group, Topic, 0, string.Empty, 7, 3000, BrokerName));
            Attempt(() => instance.ChangeInvisibleTime(Group, Topic, 0, string.Empty, 7, 3000, 3000,
                BrokerName));
            Attempt(() => instance.RecallMessage(cluster.Plain.Addr, new RecallMessageRequestHeader
            {
                ProducerGroup = Group,
                Topic = Topic,
                RecallHandle = "handle",
                Bname = BrokerName,
            }, 3000));
            instance.UnregisterClientAllBrokers(ClientId, Group, string.Empty);
        }
        finally
        {
            instance.Shutdown();
        }

        foreach (int code in NeverRewritten)
        {
            Assert.Equal(0, cluster.Vip.CountFor(code));
        }

        Assert.True(cluster.Plain.CountFor(RequestCode.HeartBeat) >= 1, "heartbeat stays on the route port");
        Assert.True(cluster.Plain.CountFor(RequestCode.UnregisterClient) >= 1,
            "unregister stays on the route port");
        Assert.True(cluster.Plain.CountFor(RequestCode.PullMessage) >= 1, "pull stays on the route port");
        Assert.True(cluster.Plain.CountFor(RequestCode.PopMessage) >= 1, "pop stays on the route port");
        Assert.True(cluster.Plain.CountFor(RequestCode.AckMessage) >= 1, "ack stays on the route port");
        Assert.True(cluster.Plain.CountFor(RequestCode.ChangeMessageInvisibletime) >= 1,
            "the invisible-time change stays on the route port");
        Assert.True(cluster.Plain.CountFor(RequestCode.RecallMessage) >= 1,
            "recall stays on the route port");
    }

    /// <summary>A shifted address must not be remembered: with the switch on, two requests
    /// of the same family still go to port-2 while the route cache itself keeps carrying the
    /// plain port.</summary>
    [Fact]
    public void ShiftIsAppliedPerRequest_NotCachedInTheRoute()
    {
        using Cluster cluster = Cluster.Start();
        MQClientInstance instance = Seeded(cluster, true);
        try
        {
            MessageQueue mq = Queue();
            Assert.Equal(7, instance.GetMaxOffset(mq, 3000));
            Assert.Equal(7, instance.GetMaxOffset(mq, 3000));
            Assert.Equal(7, instance.GetMinOffset(mq, 3000));
            Assert.Equal(cluster.Plain.Addr, instance.PublishAddrInAdmin(mq));
        }
        finally
        {
            instance.Shutdown();
        }

        Assert.Equal(2, cluster.Vip.CountFor(RequestCode.GetMaxOffset));
        Assert.Equal(1, cluster.Vip.CountFor(RequestCode.GetMinOffset));
        Assert.Equal(0, cluster.Plain.CountFor(RequestCode.GetMaxOffset));
        Assert.Equal(0, cluster.Plain.CountFor(RequestCode.GetMinOffset));
    }

    /// <summary>The rewrite is arithmetic on the route port: an address without a parsable
    /// port is passed through unchanged rather than turned into a broken target.</summary>
    [Fact]
    public void PortMinusTwo_IsExactlyTwoBelowTheRoutePort()
    {
        Assert.Equal("127.0.0.1:10909", MixAll.BrokerVipChannel(true, "127.0.0.1:10911"));
        Assert.Equal("127.0.0.1:10911", MixAll.BrokerVipChannel(false, "127.0.0.1:10911"));
        Assert.Equal("127.0.0.1:" + (10911 - 2).ToString(CultureInfo.InvariantCulture),
            MixAll.BrokerVipChannel(true, "127.0.0.1:10911"));
    }

    // ---------------------------------------------------------------- (iv) the admin facade

    /// <summary>An admin started against the cluster. The instance name carries the switch
    /// state so the two runs cannot be handed the same pooled client, which would silently
    /// reuse another cluster's name-server address.</summary>
    private static DefaultMQAdminExt StartedAdmin(Cluster cluster, string tag, bool vipOn)
    {
        var admin = new DefaultMQAdminExt("ADMIN_vip_" + tag);
        admin.SetNamesrvAddr(cluster.Plain.Addr);
        admin.VipChannelEnabled = vipOn;
        admin.Start();
        return admin;
    }

    /// <summary>Broker-wide administration reads move with the switch -- except the one that
    /// asks a broker for its own configuration, which the fast channel does not serve.</summary>
    [Fact]
    public void AdminBrokerReads_MoveExceptTheBrokerConfigRead()
    {
        using Cluster cluster = Cluster.Start();
        DefaultMQAdminExt admin = StartedAdmin(cluster, "admin_moves", true);
        try
        {
            Attempt(() => admin.FetchBrokerRuntimeStats(cluster.Plain.Addr, 3000));
            Attempt(() => admin.GetAllTopicConfig(cluster.Plain.Addr, 3000));
            Attempt(() => admin.ExamineTopicStatsByBroker(cluster.Plain.Addr, Topic));
            Attempt(() => admin.GetBrokerConfig(cluster.Plain.Addr, 3000));
        }
        finally
        {
            admin.Shutdown();
        }

        Assert.Equal(1, cluster.Vip.CountFor(RequestCode.GetBrokerRuntimeInfo));
        Assert.Equal(1, cluster.Vip.CountFor(RequestCode.GetAllTopicConfig));
        Assert.Equal(1, cluster.Vip.CountFor(RequestCode.GetTopicStatsInfo));
        Assert.Equal(0, cluster.Plain.CountFor(RequestCode.GetBrokerRuntimeInfo));
        Assert.Equal(0, cluster.Plain.CountFor(RequestCode.GetAllTopicConfig));
        Assert.Equal(0, cluster.Plain.CountFor(RequestCode.GetTopicStatsInfo));

        Assert.Equal(1, cluster.Plain.CountFor(RequestCode.GetBrokerConfig));
        Assert.Equal(0, cluster.Vip.CountFor(RequestCode.GetBrokerConfig));
    }

    /// <summary>The admin delegates the offset family to the shared client instance, so the
    /// switch has to reach that instance in both directions: once when it is set before
    /// Start() and again when it is flipped afterwards.</summary>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void AdminDelegatedOffsets_FollowTheSwitchBeforeAndAfterStart(bool vipOn)
    {
        using Cluster cluster = Cluster.Start();
        DefaultMQAdminExt admin = StartedAdmin(cluster, vipOn ? "delegated_on" : "delegated_off", vipOn);
        MessageQueue mq = Queue();
        try
        {
            Endpoint first = vipOn ? cluster.Vip : cluster.Plain;
            Endpoint other = vipOn ? cluster.Plain : cluster.Vip;

            Assert.Equal(7, admin.MaxOffset(mq));
            Assert.Equal(7, admin.MinOffset(mq));
            Assert.Equal(1, first.CountFor(RequestCode.GetMaxOffset));
            Assert.Equal(1, first.CountFor(RequestCode.GetMinOffset));

            admin.VipChannelEnabled = !vipOn;

            Assert.Equal(7, admin.MaxOffset(mq));
            Assert.Equal(7, admin.MinOffset(mq));
            Assert.Equal(1, other.CountFor(RequestCode.GetMaxOffset));
            Assert.Equal(1, other.CountFor(RequestCode.GetMinOffset));
        }
        finally
        {
            admin.Shutdown();
        }
    }
}
