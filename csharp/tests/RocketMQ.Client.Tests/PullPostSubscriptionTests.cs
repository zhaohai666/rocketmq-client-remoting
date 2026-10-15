// P5: offline unit tests for postSubscriptionWhenPull + updatePullFromWhichNode (the
// PullAPIWrapper port).
//
//
// The wire shape (the SUBSCRIPTION bit, whether the `subscription` key goes out, whether
// it is sent to the master or a slave, the COMMIT_OFFSET bit being cleared) is evidenced
// here off the socket with an in-process fake endpoint, rather than taken from the caller's
// own word.
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;
// PropertyMap is a global using alias on the src side (SortedDictionary<string,string>), so the test project has to declare it explicitly.
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class PullPostSubscriptionTests
{
    private const string Group = "GID_P5Unit";
    private const string Topic = "P5PullTopic";
    private const string Broker = "broker-a";
    private const string ClientId = "127.0.0.1@5555#1";
    private const int MaxFrame = 20 * 1024 * 1024;

    // ---------------------------------------------------------------- Fake endpoint

    /// <summary>
    /// An in-process fake endpoint: it can serve as the namesrv returning routes as well as
    /// the broker answering PULL_MESSAGE.
    /// One master and one slave are started, so "who it was sent to" is evidenced straight
    /// off the socket.
    /// </summary>
    private sealed class PullEndpoint : IDisposable
    {
        private readonly object _gate = new();
        private readonly Dictionary<string, byte[]> _routes = new();
        private readonly List<PropertyMap> _pulls = new();
        private PropertyMap _pullExt = new();
        private readonly Socket _listener;

        public string Addr { get; }

        private PullEndpoint(Socket listener)
        {
            _listener = listener;
            Addr = ((IPEndPoint)listener.LocalEndPoint!).ToString();
            new Thread(AcceptLoop) { IsBackground = true }.Start();
        }

        public static PullEndpoint Start()
        {
            var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            server.Listen(16);
            return new PullEndpoint(server);
        }

        public void AddRoute(string topic, TopicRouteData route)
        {
            lock (_gate)
            {
                _routes[topic] = route.Encode();
            }
        }

        /// <summary>Scripted PULL_MESSAGE response header (nextBeginOffset / maxOffset / suggestWhichBrokerId...).</summary>
        public void ScriptPullResponse(PropertyMap ext)
        {
            lock (_gate)
            {
                _pullExt = ext;
                _pulls.Clear();
            }
        }

        /// <summary>A snapshot of the request header of a received PULL_MESSAGE (exactly as
        /// it went on the wire).</summary>
        public List<PropertyMap> Pulls()
        {
            lock (_gate)
            {
                return new List<PropertyMap>(_pulls);
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
                // Closing the listening port needs no exception handling
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
                    return;  // after Dispose, Accept necessarily throws, this is a normal shutdown
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
                    continue;  // oneway has no response to write
                }

                byte[] wire = resp.Encode();
                stream.Write(wire, 0, wire.Length);
            }
        }

        private RemotingCommand? Respond(RemotingCommand req)
        {
            if (req.Code == RequestCode.GetRouteinfoByTopic)
            {
                string topic = req.ExtFields.TryGetValue("topic", out string? t) ? t : "";
                byte[] body;
                lock (_gate)
                {
                    if (!_routes.TryGetValue(topic, out byte[]? found))
                    {
                        return Echo(req, ResponseCode.TopicNotExist, "mock: no route");
                    }

                    body = found;
                }

                RemotingCommand routeResp = Echo(req, ResponseCode.Success, null);
                routeResp.Body = body;
                routeResp.HasBody = true;
                return routeResp;
            }

            if (req.Code == RequestCode.PullMessage)
            {
                PropertyMap ext = new(req.ExtFields);
                lock (_gate)
                {
                    _pulls.Add(ext);
                    ext = new PropertyMap(_pullExt);
                }

                RemotingCommand pullResp = Echo(req, ResponseCode.Success, null);
                foreach ((string key, string value) in ext)
                {
                    pullResp.AddExtField(key, value);
                }

                return pullResp;
            }

            // Heartbeats and the like always succeed, do not let the background thread get stuck on an error
            return req.IsOnewayRpc() ? null : Echo(req, ResponseCode.Success, null);
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

    /// <summary>An already-closed loopback port that is guaranteed to refuse connections
    /// (used to construct "the slave does not exist").</summary>
    private static string DeadAddr()
    {
        var s = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        s.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        s.Listen(1);
        string addr = ((IPEndPoint)s.LocalEndPoint!).ToString();
        s.Dispose();
        return addr;
    }

    private static PropertyMap PullRespExt(long? suggest)
    {
        var ext = new PropertyMap
        {
            ["nextBeginOffset"] = "1",
            ["minOffset"] = "0",
            ["maxOffset"] = "10",
        };
        if (suggest is not null)
        {
            ext["suggestWhichBrokerId"] = suggest.Value.ToString(System.Globalization.CultureInfo.InvariantCulture);
        }

        return ext;
    }

    // ---------------------------------------------------------------- findBrokerAddressInSubscribe

    [Fact]
    public void FindBrokerAddressInSubscribeBranches()
    {
        var addrs = new SortedDictionary<long, string>
        {
            [0] = "127.0.0.1:10911",
            [1] = "127.0.0.1:10921",
            [2] = "127.0.0.1:10931",
        };

        Assert.Equal(("127.0.0.1:10911", false),
            MQClientInstance.FindBrokerAddressInSubscribe(addrs, 0));
        Assert.Equal(("127.0.0.1:10921", true),
            MQClientInstance.FindBrokerAddressInSubscribe(addrs, 1));

        var sparse = new SortedDictionary<long, string>
        {
            [0] = "127.0.0.1:10911",
            [2] = "127.0.0.1:10931",
        };
        Assert.Equal(("127.0.0.1:10931", true),
            MQClientInstance.FindBrokerAddressInSubscribe(sparse, 1));

        Assert.Equal(("127.0.0.1:10911", false),
            MQClientInstance.FindBrokerAddressInSubscribe(addrs, 9));
        Assert.Equal(("127.0.0.1:10921", true),
            MQClientInstance.FindBrokerAddressInSubscribe(
                new SortedDictionary<long, string> { [1] = "127.0.0.1:10921" }, 3));

        // onlyThisBroker: better to come up short than to over-deliver
        Assert.Equal((string.Empty, false),
            MQClientInstance.FindBrokerAddressInSubscribe(addrs, 9, onlyThisBroker: true));
        Assert.Equal((string.Empty, false),
            MQClientInstance.FindBrokerAddressInSubscribe(
                new SortedDictionary<long, string>(), 0));
    }

    // ---------------------------------------------------------------- The wire

    [Fact]
    public void PullToSlaveClearsCommitOffsetAndOmitsSubscription()
    {
        using PullEndpoint namesrv = PullEndpoint.Start();
        using PullEndpoint master = PullEndpoint.Start();
        using PullEndpoint slave = PullEndpoint.Start();
        namesrv.AddRoute(Topic, RouteWithMasterAndSlave(master.Addr, slave.Addr));
        slave.ScriptPullResponse(PullRespExt(suggest: 0));

        MQClientInstance instance = NewInstanceWithRoute(namesrv);
        try
        {
            var mq = new MessageQueue(Topic, Broker, 0);
            int sysFlag = PullSysFlag.BuildSysFlag(commitOffset: true, suspend: true,
                subscription: false, classFilter: false);
            Assert.True(PullSysFlag.HasCommitOffsetFlag(sysFlag));

            PullResult result = instance.PullMessage(Group, mq, 0, 32, sysFlag, 0,
                "TagA", 0, ExpressionType.TAG, 3000, -1, 15000,
                /*addrIn=*/null, /*requestSource=*/0,
                /*brokerId=*/MixAll.MasterId + 1);

            Assert.Single(slave.Pulls());
            Assert.Empty(master.Pulls());
            PropertyMap sent = slave.Pulls()[0];
            int sentFlag = int.Parse(sent["sysFlag"], System.Globalization.CultureInfo.InvariantCulture);
            Assert.False(PullSysFlag.HasCommitOffsetFlag(sentFlag), "a single COMMIT_OFFSET on a slave is meaningless");
            Assert.True(PullSysFlag.HasSuspendFlag(sentFlag), "the suspend bit stays");
            Assert.False(sent.ContainsKey("subscription"),
                "when the SUBSCRIPTION bit is off that field never enters extFields (null -> the field is dropped)");
            // the response header's suggestWhichBrokerId is passed through to the caller, for writing back pullFromWhichNodeTable
            Assert.Equal(0L, result.SuggestWhichBrokerId);
        }
        finally
        {
            instance.Shutdown();
        }
    }

    /// <summary>
    /// brokerId=0 (the master): the request goes to the master and the COMMIT_OFFSET bit is
    /// kept; only when the SUBSCRIPTION bit is set does `subscription` go out, carrying the
    /// expression the caller gave; when an old broker sends no suggestWhichBrokerId, null
    /// is passed through (the caller books it as master=0).
    /// </summary>
    [Fact]
    public void PullToMasterKeepsCommitOffsetAndPostsSubscription()
    {
        using PullEndpoint namesrv = PullEndpoint.Start();
        using PullEndpoint master = PullEndpoint.Start();
        using PullEndpoint slave = PullEndpoint.Start();
        namesrv.AddRoute(Topic, RouteWithMasterAndSlave(master.Addr, slave.Addr));
        master.ScriptPullResponse(PullRespExt(suggest: null));

        MQClientInstance instance = NewInstanceWithRoute(namesrv);
        try
        {
            var mq = new MessageQueue(Topic, Broker, 0);
            int sysFlag = PullSysFlag.BuildSysFlag(commitOffset: true, suspend: true,
                subscription: true, classFilter: false);

            PullResult result = instance.PullMessage(Group, mq, 3, 32, sysFlag, 0,
                "TagA||TagB", 0, ExpressionType.TAG, 3000, -1, 15000,
                /*addrIn=*/null, /*requestSource=*/0, /*brokerId=*/MixAll.MasterId);

            Assert.Single(master.Pulls());
            Assert.Empty(slave.Pulls());
            PropertyMap sent = master.Pulls()[0];
            int sentFlag = int.Parse(sent["sysFlag"], System.Globalization.CultureInfo.InvariantCulture);
            Assert.True(PullSysFlag.HasCommitOffsetFlag(sentFlag), "on the master the offset is committed as usual");
            Assert.True(PullSysFlag.HasSubscriptionFlag(sentFlag));
            Assert.Equal("TagA||TagB", sent["subscription"]);
            Assert.Equal("3", sent["queueOffset"]);
            Assert.Null(result.SuggestWhichBrokerId);
            Assert.Equal(1L, result.NextBeginOffset);
        }
        finally
        {
            instance.Shutdown();
        }
    }

    [Fact]
    public void MissingSlaveFallsBackToMasterAndKeepsCommitOffset()
    {
        using PullEndpoint namesrv = PullEndpoint.Start();
        using PullEndpoint master = PullEndpoint.Start();
        namesrv.AddRoute(Topic, RouteWithMasterOnly(master.Addr));
        master.ScriptPullResponse(PullRespExt(suggest: null));

        MQClientInstance instance = NewInstanceWithRoute(namesrv);
        try
        {
            var mq = new MessageQueue(Topic, Broker, 0);
            int sysFlag = PullSysFlag.BuildSysFlag(commitOffset: true, suspend: true,
                subscription: false, classFilter: false);

            instance.PullMessage(Group, mq, 0, 32, sysFlag, 0, "TagA", 0,
                ExpressionType.TAG, 3000, -1, 15000,
                /*addrIn=*/null, /*requestSource=*/0, /*brokerId=*/3);

            PropertyMap sent = Assert.Single(master.Pulls());
            int sentFlag = int.Parse(sent["sysFlag"], System.Globalization.CultureInfo.InvariantCulture);
            Assert.True(PullSysFlag.HasCommitOffsetFlag(sentFlag),
                "the fallback landed on the master, so the offset is committed as usual (IsSlave judges by the id that was matched, not the request's brokerId)");
        }
        finally
        {
            instance.Shutdown();
        }
    }

    // ---------------------------------------------------------------- Consumer configuration and bookkeeping

    [Fact]
    public void ConsumerSubscriptionGatingFollowsLegacyDefault()
    {
        var consumer = new DefaultMQPushConsumer(Group);
        var sub = new SubscriptionData(Topic, "TagA");
        Assert.False(consumer.PostSubscriptionWhenPull);
        Assert.Null(consumer.PullSubscriptionExpressionForTest(sub));

        consumer.PostSubscriptionWhenPull = true;
        Assert.Equal("TagA", consumer.PullSubscriptionExpressionForTest(sub));

        var blank = new SubscriptionData(Topic, "");
        Assert.Equal("*", consumer.PullSubscriptionExpressionForTest(blank));

        // Class filtering mode: the expression is a filter class name and TAG filtering on the broker side would misjudge it, so it is not sent even when the switch is on
        var classMode = new SubscriptionData(Topic, "com.example.MyFilter") { ClassFilterMode = true };
        Assert.Null(consumer.PullSubscriptionExpressionForTest(classMode));
    }

    [Fact]
    public void PullFromWhichNodeDefaultsToMasterAndRoundTrips()
    {
        var consumer = new DefaultMQPushConsumer(Group);
        var mq = new MessageQueue(Topic, Broker, 0);
        var other = new MessageQueue(Topic, Broker, 1);
        string key = DefaultMQPushConsumer.OffsetKeyForTest(mq);
        string otherKey = DefaultMQPushConsumer.OffsetKeyForTest(other);

        Assert.Equal(MixAll.MasterId, consumer.PullFromWhichNodeForTest(key));

        consumer.UpdatePullFromWhichNodeForTest(key, MixAll.MasterId + 1);
        Assert.Equal(MixAll.MasterId + 1, consumer.PullFromWhichNodeForTest(key));
        consumer.UpdatePullFromWhichNodeForTest(key, null);
        Assert.Equal(MixAll.MasterId, consumer.PullFromWhichNodeForTest(key));

        consumer.UpdatePullFromWhichNodeForTest(key, 2);
        Assert.Equal(2, consumer.PullFromWhichNodeForTest(key));
        Assert.Equal(MixAll.MasterId, consumer.PullFromWhichNodeForTest(otherKey));
    }

    // ---------------------------------------------------------------- Fixtures

    private static TopicRouteData RouteWithMasterAndSlave(string masterAddr, string slaveAddr)
    {
        var route = new TopicRouteData();
        route.QueueDatas.Add(new QueueData(Broker, 1, 1, PermName.PermRead | PermName.PermWrite, 0));
        route.BrokerDatas.Add(new BrokerData("DefaultCluster", Broker,
            new SortedDictionary<long, string>
            {
                [MixAll.MasterId] = masterAddr,
                [MixAll.MasterId + 1] = slaveAddr,
            }));
        return route;
    }

    private static TopicRouteData RouteWithMasterOnly(string masterAddr)
    {
        var route = new TopicRouteData();
        route.QueueDatas.Add(new QueueData(Broker, 1, 1, PermName.PermRead | PermName.PermWrite, 0));
        route.BrokerDatas.Add(new BrokerData("DefaultCluster", Broker,
            new SortedDictionary<long, string> { [MixAll.MasterId] = masterAddr }));
        return route;
    }

    /// <summary>Builds the instance and stuffs the Topic route into the cache (a real RPC
    /// round trip through the namesrv).</summary>
    private static MQClientInstance NewInstanceWithRoute(PullEndpoint namesrv)
    {
        var instance = new MQClientInstance(ClientId, new List<string> { namesrv.Addr });
        Assert.True(instance.UpdateTopicRouteInfoFromNameServer(Topic), "the route must really enter the cache");
        return instance;
    }

    /// <summary>The guard on the dead-port path
    /// (<c>FindBrokerAddressInSubscribe</c> must not treat a dead address as a hit).</summary>
    [Fact]
    public void DeadPortIsStillReturnedAsTheHit()
    {
        string dead = DeadAddr();
        var addrs = new SortedDictionary<long, string> { [MixAll.MasterId] = dead };
        Assert.Equal((dead, false), MQClientInstance.FindBrokerAddressInSubscribe(addrs, 0));
    }
}
