// Offline unit tests on the publish address side: the two rules "master only" and "fall
// back to a slave" are pinned down separately.
using System.Buffers.Binary;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;
// PropertyMap is a global using alias on the src side (SortedDictionary<string,string>), so the test project has to declare it explicitly.
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class PublishRouteMasterTests
{
    private const string Broker = "broker-a";
    private const string Topic = "PublishRouteMasterUnitTopic";
    private const long SlaveId = MixAll.MasterId + 1;

    // ---------------------------------------------------------------- Fake endpoints

    /// <summary>
    /// An in-process fake endpoint: it can serve as the namesrv returning routes as well
    /// as the broker answering offset queries.
    /// One master and one slave are started, so "which one the request landed on" is
    /// evidenced straight off the socket.
    /// </summary>
    private sealed class RouteEndpoint : IDisposable
    {
        /// <summary>The response value of each of the four queries (all different, proving that this path's data is what came back).</summary>
        public const long MaxOffsetReply = 7;
        public const long MinOffsetReply = 3;
        public const long SearchOffsetReply = 5;
        public const long EarliestReply = 1700000000000;

        private readonly object _gate = new();
        private readonly Dictionary<string, byte[]> _routes = new();
        private readonly List<(int Code, PropertyMap Ext)> _requests = new();
        private readonly Socket _listener;

        public string Addr { get; }

        /// <summary>The response offset of QUERY_CONSUMER_OFFSET(14) (one value on the master, a different one on the slave).</summary>
        public long OffsetReply { get; set; }

        private RouteEndpoint(Socket listener)
        {
            _listener = listener;
            Addr = ((IPEndPoint)listener.LocalEndPoint!).ToString();
            new Thread(AcceptLoop) { IsBackground = true }.Start();
        }

        public static RouteEndpoint Start()
        {
            var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            server.Listen(16);
            return new RouteEndpoint(server);
        }

        public void AddRoute(string topic, TopicRouteData route)
        {
            lock (_gate)
            {
                _routes[topic] = route.Encode();
            }
        }

        public int CountRequests(int code)
        {
            lock (_gate)
            {
                return _requests.Count(r => r.Code == code);
            }
        }

        /// <summary>How many of the four offset queries (29/30/31/32) this endpoint received in total -- "did any wire reach this one".</summary>
        public int CountOffsetQueries()
        {
            lock (_gate)
            {
                return _requests.Count(r => r.Code is RequestCode.SearchOffsetByTimestamp
                    or RequestCode.GetMaxOffset or RequestCode.GetMinOffset
                    or RequestCode.GetEarliestMsgStoretime);
            }
        }

        public PropertyMap? LastRequest(int code)
        {
            lock (_gate)
            {
                for (int i = _requests.Count - 1; i >= 0; --i)
                {
                    if (_requests[i].Code == code)
                    {
                        return _requests[i].Ext;
                    }
                }

                return null;
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
                if (total <= 0 || total > 20 * 1024 * 1024)
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

                lock (_gate)
                {
                    _requests.Add((req.Code, new PropertyMap(req.ExtFields)));
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
                string topic = req.ExtFields.TryGetValue("topic", out string? t) ? t : string.Empty;
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

            if (req.Code is RequestCode.GetMaxOffset or RequestCode.GetMinOffset
                or RequestCode.SearchOffsetByTimestamp)
            {
                long value = req.Code switch
                {
                    RequestCode.GetMaxOffset => MaxOffsetReply,
                    RequestCode.GetMinOffset => MinOffsetReply,
                    _ => SearchOffsetReply,
                };
                RemotingCommand offsetResp = Echo(req, ResponseCode.Success, null);
                offsetResp.AddExtField("offset", value.ToString(CultureInfo.InvariantCulture));
                return offsetResp;
            }

            if (req.Code == RequestCode.GetEarliestMsgStoretime)
            {
                RemotingCommand earliestResp = Echo(req, ResponseCode.Success, null);
                earliestResp.AddExtField("timestamp",
                    EarliestReply.ToString(CultureInfo.InvariantCulture));
                return earliestResp;
            }

            if (req.Code == RequestCode.QueryConsumerOffset)
            {
                RemotingCommand offsetResp = Echo(req, ResponseCode.Success, null);
                offsetResp.AddExtField("offset", OffsetReply.ToString(CultureInfo.InvariantCulture));
                return offsetResp;
            }

            if (req.Code == RequestCode.LockBatchMq)
            {
                // Echo the request body's mqSet back as lockOKMQSet: a non-empty returned lock set proves "this send really landed"
                var okSet = JsonValue.MakeArray();
                if (req.Body is { Length: > 0 }
                    && Json.TryParse(Encoding.UTF8.GetString(req.Body), out JsonValue body, out _)
                    && body is not null)
                {
                    JsonValue mqSet = body.Get("mqSet");
                    for (int i = 0; mqSet.IsArray && i < mqSet.Size(); ++i)
                    {
                        okSet.PushArray(mqSet.At(i));
                    }
                }

                var lockBody = JsonValue.MakeObject();
                lockBody.Set("lockOKMQSet", okSet);
                RemotingCommand lockResp = Echo(req, ResponseCode.Success, null);
                lockResp.Body = Encoding.UTF8.GetBytes(lockBody.Dump());
                lockResp.HasBody = true;
                return lockResp;
            }

            if (req.Code == RequestCode.PopMessage)
            {
                // A long poll coming back empty-handed is normal: reply 210 POLLING_TIMEOUT and the client treats it as PollingNotFound
                return Echo(req, ResponseCode.PollingTimeout, "mock: no message");
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

    // ---------------------------------------------------------------- Fixtures

    private static TopicRouteData RouteOf(string brokerName, params (long BrokerId, string Addr)[] addrs)
    {
        var route = new TopicRouteData();
        route.QueueDatas.Add(new QueueData(brokerName, 2, 2,
            PermName.PermRead | PermName.PermWrite, 0));
        var map = new SortedDictionary<long, string>();
        foreach ((long id, string addr) in addrs)
        {
            map[id] = addr;
        }

        route.BrokerDatas.Add(new BrokerData("DefaultCluster", brokerName, map));
        return route;
    }

    /// <summary>Builds the instance and stuffs the Topic route into the cache (a real RPC
    /// round trip through the fake namesrv).</summary>
    private static MQClientInstance NewInstanceWithRoute(RouteEndpoint namesrv, string clientId)
    {
        var instance = new MQClientInstance(clientId, new List<string> { namesrv.Addr });
        Assert.True(instance.UpdateTopicRouteInfoFromNameServer(Topic), "the route must really enter the cache");
        return instance;
    }

    private static void AssertNotExist(MQClientException e)
    {
        Assert.Equal("The broker[" + Broker + "] not exist", e.Message);
        Assert.Equal(-1, e.ResponseCode);
    }

    // ---------------------------------------------------------------- Address resolution

    /// <summary>
    /// <c>FindBrokerAddressInPublish</c> only accepts brokerId=0; on the same route the
    /// "prefer the master, fall back to another one when there is none" rule
    /// (<c>BrokerAddrOf</c>, used by heartbeat/pull) must be able to reach a slave -- the
    /// two rules must not be merged into one.
    /// </summary>
    [Fact]
    public void PublishLookupTakesOnlyTheMasterWhileBrokerAddrOfFallsBack()
    {
        using RouteEndpoint namesrv = RouteEndpoint.Start();
        using RouteEndpoint slave = RouteEndpoint.Start();
        using RouteEndpoint master = RouteEndpoint.Start();
        namesrv.AddRoute(Topic, RouteOf(Broker, (SlaveId, slave.Addr)));

        MQClientInstance instance = NewInstanceWithRoute(namesrv, "prm-master-only@1");
        try
        {
            // after the shutdown shape: in the flat table broker-a only has brokerId=1 left
            Assert.Equal(string.Empty, instance.FindBrokerAddressInPublish(Broker));
            // Negative control: the fallback rule (used by heartbeat/pull/offset query) must be able to reach the slave address on **the same route**
            Assert.Equal(slave.Addr, instance.BrokerAddrOf(Broker));

            // The master registers again: the publish rule resolves it immediately (it skips "there is no master", not the name broker-a)
            namesrv.AddRoute(Topic, RouteOf(Broker, (MixAll.MasterId, master.Addr), (SlaveId, slave.Addr)));
            Assert.True(instance.UpdateTopicRouteInfoFromNameServer(Topic));
            Assert.Equal(master.Addr, instance.FindBrokerAddressInPublish(Broker));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    [Fact]
    public void PublishAddrForRefreshesTheRouteThenRechecks()
    {
        using RouteEndpoint namesrv = RouteEndpoint.Start();
        using RouteEndpoint master = RouteEndpoint.Start();
        var instance = new MQClientInstance("prm-refresh@1", new List<string> { namesrv.Addr });
        try
        {
            // This topic has not been seen before: that "not found -> refresh the route -> look again" really has to happen
            namesrv.AddRoute(Topic, RouteOf(Broker, (MixAll.MasterId, master.Addr)));
            Assert.Equal(master.Addr, instance.PublishAddrFor(Broker, Topic));
            Assert.Equal(1, namesrv.CountRequests(RequestCode.GetRouteinfoByTopic));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    /// <summary>The master is down (only brokerId=1 left in the route): we report "broker
    /// does not exist" locally and not a single broker request is sent.</summary>
    [Fact]
    public void PublishAddrForReportsNotExistWhenTheMasterIsGone()
    {
        using RouteEndpoint namesrv = RouteEndpoint.Start();
        using RouteEndpoint slave = RouteEndpoint.Start();
        using RouteEndpoint master = RouteEndpoint.Start();
        namesrv.AddRoute(Topic, RouteOf(Broker, (SlaveId, slave.Addr)));

        MQClientInstance instance = NewInstanceWithRoute(namesrv, "prm-down@1");
        try
        {
            int before = namesrv.CountRequests(RequestCode.GetRouteinfoByTopic);
            AssertNotExist(Assert.Throws<MQClientException>(
                () => instance.PublishAddrFor(Broker, Topic)));
            Assert.Equal(before + 1, namesrv.CountRequests(RequestCode.GetRouteinfoByTopic));
            // Not one broker wire was sent: on a slave that would be "send it and get rejected", here nothing is sent at all
            Assert.Equal(0, slave.CountOffsetQueries());

            // Negative control: what is skipped is "there is no master", not the name broker-a -- it resolves as soon as a master registers
            namesrv.AddRoute(Topic, RouteOf(Broker, (MixAll.MasterId, master.Addr), (SlaveId, slave.Addr)));
            Assert.Equal(master.Addr, instance.PublishAddrFor(Broker, Topic));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    /// <summary>This brokerName is not in the route at all (typo / decommissioned): also
    /// reports not exist, it does not fall back to another broker.</summary>
    [Fact]
    public void PublishAddrForReportsNotExistForAnUnknownBroker()
    {
        using RouteEndpoint namesrv = RouteEndpoint.Start();
        using RouteEndpoint other = RouteEndpoint.Start();
        namesrv.AddRoute(Topic, RouteOf("broker-b", (MixAll.MasterId, other.Addr)));

        MQClientInstance instance = NewInstanceWithRoute(namesrv, "prm-unknown@1");
        try
        {
            AssertNotExist(Assert.Throws<MQClientException>(
                () => instance.PublishAddrFor(Broker, Topic)));
            // Negative control: the flat table really does have it (broker-b is found); only the broker-a entry is missing
            Assert.Equal(other.Addr, instance.FindBrokerAddressInPublish("broker-b"));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    // ---------------------------------------------------------------- Admin-side offset queries

    /// <summary>
    /// The four offset queries of <c>MQAdminImpl:195/214/232/250</c> follow one rule: only
    /// the master is contacted, and with no master it reports not exist and sends not a
    /// single wire (the old behaviour was to fall back to a slave and finish the query
    /// there, silently handing back "the master's data").
    /// </summary>
    [Fact]
    public void AdminOffsetQueriesAreMasterOnlyToo()
    {
        using RouteEndpoint namesrv = RouteEndpoint.Start();
        using RouteEndpoint slave = RouteEndpoint.Start();
        using RouteEndpoint master = RouteEndpoint.Start();
        namesrv.AddRoute(Topic, RouteOf(Broker, (SlaveId, slave.Addr)));

        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(namesrv.Addr);
        admin.Start();
        try
        {
            var mq = new MessageQueue(Topic, Broker, 0);

            AssertNotExist(Assert.Throws<MQClientException>(() => admin.MaxOffset(mq)));
            AssertNotExist(Assert.Throws<MQClientException>(() => admin.MinOffset(mq)));
            AssertNotExist(Assert.Throws<MQClientException>(
                () => admin.SearchLowerBoundaryOffset(mq, 1700000000000)));
            AssertNotExist(Assert.Throws<MQClientException>(() => admin.EarliestMsgStoreTime(mq)));
            Assert.Equal(0, slave.CountOffsetQueries());

            // Negative control: once the master is back all four are found, and the requests land on the **master** address (not on that slave from the route)
            namesrv.AddRoute(Topic, RouteOf(Broker, (MixAll.MasterId, master.Addr), (SlaveId, slave.Addr)));
            admin.Client().UpdateTopicRouteInfoFromNameServer(Topic);
            Assert.Equal(RouteEndpoint.MaxOffsetReply, admin.MaxOffset(mq));
            Assert.Equal(RouteEndpoint.MinOffsetReply, admin.MinOffset(mq));
            Assert.Equal(RouteEndpoint.SearchOffsetReply,
                admin.SearchLowerBoundaryOffset(mq, 1700000000000));
            Assert.Equal(RouteEndpoint.EarliestReply, admin.EarliestMsgStoreTime(mq));
            Assert.Equal(4, master.CountOffsetQueries());
            Assert.Equal(0, slave.CountOffsetQueries());
        }
        finally
        {
            admin.Shutdown();
        }
    }

    // ---------------------------------------------------------------- Subscribe rule (#104)

    [Fact]
    public void OrderlyLocksSkipTheBrokerWhenTheMasterIsGone()
    {
        using RouteEndpoint namesrv = RouteEndpoint.Start();
        using RouteEndpoint slave = RouteEndpoint.Start();
        using RouteEndpoint master = RouteEndpoint.Start();
        namesrv.AddRoute(Topic, RouteOf(Broker, (SlaveId, slave.Addr)));

        MQClientInstance instance = NewInstanceWithRoute(namesrv, "prm-lock@1");
        try
        {
            var mqs = new List<MessageQueue> { new(Topic, Broker, 0), new(Topic, Broker, 1) };
            int before = namesrv.CountRequests(RequestCode.GetRouteinfoByTopic);

            Assert.Empty(instance.LockBatchMq("GID_prm_lock", "prm-lock@1", mqs));
            instance.UnlockBatchMq("GID_prm_lock", "prm-lock@1", mqs);
            Assert.Equal(0, slave.CountRequests(RequestCode.LockBatchMq));
            Assert.Equal(0, slave.CountRequests(RequestCode.UnlockBatchMq));
            Assert.Equal(before, namesrv.CountRequests(RequestCode.GetRouteinfoByTopic));

            // Negative control: once the master is back both the lock and the unlock land on the master address, and the returned lock set is exactly the two requested queues
            namesrv.AddRoute(Topic, RouteOf(Broker, (MixAll.MasterId, master.Addr), (SlaveId, slave.Addr)));
            instance.UpdateTopicRouteInfoFromNameServer(Topic);
            Assert.Equal(2, instance.LockBatchMq("GID_prm_lock", "prm-lock@1", mqs).Count);
            instance.UnlockBatchMq("GID_prm_lock", "prm-lock@1", mqs);
            Assert.Equal(1, master.CountRequests(RequestCode.LockBatchMq));
            Assert.Equal(1, master.CountRequests(RequestCode.UnlockBatchMq));
            Assert.Equal(0, slave.CountRequests(RequestCode.LockBatchMq));
            Assert.Equal(0, slave.CountRequests(RequestCode.UnlockBatchMq));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    [Fact]
    public void PopMessageIsMasterOnlyAndReportsNotExist()
    {
        using RouteEndpoint namesrv = RouteEndpoint.Start();
        using RouteEndpoint slave = RouteEndpoint.Start();
        using RouteEndpoint master = RouteEndpoint.Start();
        namesrv.AddRoute(Topic, RouteOf(Broker, (SlaveId, slave.Addr)));

        MQClientInstance instance = NewInstanceWithRoute(namesrv, "prm-pop@1");
        try
        {
            int before = namesrv.CountRequests(RequestCode.GetRouteinfoByTopic);
            AssertNotExist(Assert.Throws<MQClientException>(() => instance.PopMessage(
                "GID_prm_pop", Topic, 0, 1, 30000L, 1000L, 0, brokerNameIn: Broker)));
            // The route must have been refreshed once before the error (popAsync's findBrokerAddressInSubscribe + look again)
            Assert.Equal(before + 1, namesrv.CountRequests(RequestCode.GetRouteinfoByTopic));
            Assert.Equal(0, slave.CountRequests(RequestCode.PopMessage));

            // Negative control: after the master registers (this send incidentally triggers one route refresh) the request lands on the master address and not one reaches the slave
            namesrv.AddRoute(Topic, RouteOf(Broker, (MixAll.MasterId, master.Addr), (SlaveId, slave.Addr)));
            PopResult result = instance.PopMessage(
                "GID_prm_pop", Topic, 0, 1, 30000L, 1000L, 0, brokerNameIn: Broker);
            Assert.Equal(PopStatus.PollingNotFound, result.Status);
            Assert.Equal(1, master.CountRequests(RequestCode.PopMessage));
            Assert.Equal(0, slave.CountRequests(RequestCode.PopMessage));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    [Fact]
    public void ConsumerOffsetFallsBackToTheSlaveAfterARefresh()
    {
        using RouteEndpoint namesrv = RouteEndpoint.Start();
        using RouteEndpoint slave = RouteEndpoint.Start();
        using RouteEndpoint master = RouteEndpoint.Start();
        slave.OffsetReply = 424242;
        master.OffsetReply = 111;
        namesrv.AddRoute(Topic, RouteOf(Broker, (SlaveId, slave.Addr)));

        MQClientInstance instance = NewInstanceWithRoute(namesrv, "prm-offset@1");
        try
        {
            var mq = new MessageQueue(Topic, Broker, 0);
            int before = namesrv.CountRequests(RequestCode.GetRouteinfoByTopic);

            // No master: fall back to a slave to finish the query, and the value that comes back is the **slave's**
            Assert.True(instance.QueryConsumerOffset("GID_prm_offset", mq, out long offset));
            Assert.Equal(424242L, offset);
            Assert.Equal(1, slave.CountRequests(RequestCode.QueryConsumerOffset));
            Assert.Equal(before + 1, namesrv.CountRequests(RequestCode.GetRouteinfoByTopic));

            // Negative control: once the master is back only the master is contacted (the slave's counter stops growing)
            namesrv.AddRoute(Topic, RouteOf(Broker, (MixAll.MasterId, master.Addr), (SlaveId, slave.Addr)));
            instance.UpdateTopicRouteInfoFromNameServer(Topic);
            Assert.True(instance.QueryConsumerOffset("GID_prm_offset", mq, out offset));
            Assert.Equal(111L, offset);
            Assert.Equal(1, master.CountRequests(RequestCode.QueryConsumerOffset));
            Assert.Equal(1, slave.CountRequests(RequestCode.QueryConsumerOffset));
        }
        finally
        {
            instance.Shutdown();
        }

        // broker-a is not in the route at all (not even a slave): still not found after one refresh => we report not exist locally
        using RouteEndpoint namesrv2 = RouteEndpoint.Start();
        using RouteEndpoint other = RouteEndpoint.Start();
        namesrv2.AddRoute(Topic, RouteOf("broker-b", (MixAll.MasterId, other.Addr)));
        MQClientInstance instance2 = NewInstanceWithRoute(namesrv2, "prm-offset@2");
        try
        {
            var mq = new MessageQueue(Topic, Broker, 0);
            AssertNotExist(Assert.Throws<MQClientException>(
                () => instance2.QueryConsumerOffset("GID_prm_offset", mq, out _)));
            Assert.Equal(0, other.CountRequests(RequestCode.QueryConsumerOffset));
        }
        finally
        {
            instance2.Shutdown();
        }
    }
}
