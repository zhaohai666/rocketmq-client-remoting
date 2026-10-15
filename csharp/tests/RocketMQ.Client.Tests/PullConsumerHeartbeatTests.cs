// Offline unit tests for the **heartbeat** of pull mode / lite pull consumers -- no cluster needed.
//
//
// Why it brings its own fake cluster: the heartbeat has to "reach both the master and the
// slave", while MockCluster's route is "one name per broker, master only", so that
// criterion cannot be constructed on it (the same reason as in ProducerUnregisterTests).
using System.Buffers.Binary;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

using Xunit;

namespace RocketMQ.Client.Tests;

public class PullConsumerHeartbeatTests
{
    private const string Topic = "PullHbNetUnitTopic";
    private const string Group = "GID_pullhb_net_unit";
    private const string BrokerName = "broker-hb";

    /// <summary>The evidence of one request on the wire: which broker it landed on, which
    /// code, extFields / body.</summary>
    private sealed record Frame(string Addr, int Code, PropertyMap Ext, byte[] Body);

    private sealed class NoopListener : IMessageQueueListener
    {
        public void MessageQueueChanged(string topic, IReadOnlyList<MessageQueue> mqAll,
            IReadOnlyList<MessageQueue> mqDivided)
        {
        }
    }

    /// <summary>
    /// A fake cluster speaking only the remoting protocol: 1 namesrv plus two really
    /// listening brokers under the same brokerName, a master(0) and a slave(1). The broker
    /// replies SUCCESS with an empty body to every request and records all frames in arrival
    /// order.
    /// </summary>
    private sealed class MasterSlaveCluster : IDisposable
    {
        private readonly object _gate = new();
        private readonly List<Socket> _listeners = new();
        private readonly List<Frame> _frames = new();

        public string NamesrvAddr { get; private init; } = string.Empty;
        public string MasterAddr { get; private init; } = string.Empty;
        public string SlaveAddr { get; private init; } = string.Empty;

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

        public List<Frame> Frames(int code)
        {
            lock (_gate)
            {
                return _frames.Where(f => f.Code == code).ToList();
            }
        }

        public int CountAt(string addr, int code) => Frames(code).Count(f => f.Addr == addr);

        private RemotingCommand? NamesrvRespond(RemotingCommand req)
        {
            Record(NamesrvAddr, req);
            if (req.Code == RequestCode.GetRouteinfoByTopic)
            {
                RemotingCommand resp = Respond(req, ResponseCode.Success, null);
                // the master and slave addresses are only known at runtime, so the route body is assembled here
                routeBody ??= BuildRouteBody();
                resp.Body = routeBody;
                resp.HasBody = true;
                return resp;
            }

            return Respond(req, ResponseCode.Success, null);
        }

        private byte[]? routeBody;

        private byte[] BuildRouteBody()
        {
            // replace the placeholder name in the TopicRoute with the real address
            var route = new TopicRouteData();
            route.QueueDatas.Add(new QueueData(BrokerName, 2, 2,
                PermName.PermRead | PermName.PermWrite, 0));
            route.BrokerDatas.Add(new BrokerData("HbCluster", BrokerName,
                new SortedDictionary<long, string> { { 0L, MasterAddr }, { 1L, SlaveAddr } }));
            return route.Encode();
        }

        private RemotingCommand? BrokerRespond(string addr, RemotingCommand req)
        {
            Record(addr, req);
            return req.IsOnewayRpc() ? null : Respond(req, ResponseCode.Success, null);
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
                }
            }
        }
    }

    private static RemotingCommand Respond(RemotingCommand req, int code, string? remark)
    {
        RemotingCommand resp = RemotingCommand.CreateResponseCommand(code, remark);
        resp.Opaque = req.Opaque;
        resp.SerializeTypeCurrentRpc = req.SerializeTypeCurrentRpc;
        return resp;
    }

    private static Socket BindLoopback()
    {
        var listener = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        listener.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        listener.Listen(64);
        return listener;
    }

    private static string EndPointOf(Socket listener) =>
        ((IPEndPoint)listener.LocalEndPoint!).ToString();

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
                    return;
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
                        }
                    }
                })
                { IsBackground = true }.Start();
            }
        })
        { IsBackground = true }.Start();
    }

    private const int MaxFrame = 20 * 1024 * 1024;

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
            if (total <= 0 || total > MaxFrame)
            {
                return;
            }

            var frame = new byte[total];
            if (!ReadFully(stream, frame, 0, total))
            {
                return;
            }

            var wire = new byte[4 + total];
            Buffer.BlockCopy(lenBuf, 0, wire, 0, 4);
            Buffer.BlockCopy(frame, 0, wire, 4, total);
            if (!RemotingCommand.TryDecode(wire, out RemotingCommand req, out _))
            {
                return;
            }

            RemotingCommand? resp = respond(req);
            if (resp is null)
            {
                continue;
            }

            byte[] outBytes = resp.Encode();
            stream.Write(outBytes, 0, outBytes.Length);
            stream.Flush();
        }
    }

    private static bool ReadFully(NetworkStream stream, byte[] buf, int offset, int count)
    {
        int got = 0;
        while (got < count)
        {
            int n = stream.Read(buf, offset + got, count - got);
            if (n <= 0)
            {
                return false;
            }

            got += n;
        }

        return true;
    }

    private static bool WaitFor(Func<bool> cond, int timeoutMs = 5000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (cond())
            {
                return true;
            }

            Thread.Sleep(20);
        }

        return cond();
    }

    private static ConsumerData DecodeFirstConsumerData(byte[] body)
    {
        JsonValue json = Json.Parse(Encoding.UTF8.GetString(body));
        HeartbeatData hb = HeartbeatData.FromJson(json);
        Assert.NotEmpty(hb.ConsumerDataSet);
        return hb.ConsumerDataSet[0];
    }

    // ---------------------------------------------------------------- 1. Registered on start

    [Fact]
    public void Start_AnnouncesTheGroupWithTheLegacyShape()
    {
        using var cluster = MasterSlaveCluster.Start();
        var consumer = new DefaultMQPullConsumer(Group);
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.SetInstanceName("pull-hb-net-unit");
        consumer.UnitMode = true;
        consumer.RegisterMessageQueueListener(Topic, new NoopListener());
        consumer.Start();
        try
        {
            Assert.True(WaitFor(() => cluster.CountAt(cluster.MasterAddr, RequestCode.HeartBeat) >= 1),
                "the master receives HEART_BEAT after Start()");
            Assert.Equal(1, consumer.HeartbeatCount);

            Frame hb = cluster.Frames(RequestCode.HeartBeat).First(f => f.Addr == cluster.MasterAddr);
            ConsumerData cd = DecodeFirstConsumerData(hb.Body);
            Assert.Equal(Group, cd.GroupName);
            // DefaultMQPullConsumerImpl:348 / :353
            Assert.Equal(ConsumeType.ConsumeActively, cd.ConsumeType);
            Assert.Equal(ConsumeFromWhere.ConsumeFromLastOffset, cd.ConsumeFromWhere);
            Assert.Equal(MessageModel.Clustering, cd.MessageModel);
            Assert.True(cd.UnitMode, "unitMode is reported faithfully (MQClientInstance:1039)");

            // the subscription set comes from registerTopics (subscriptions():357-385)
            SubscriptionData sub = Assert.Single(cd.SubscriptionDataSet);
            Assert.Equal(Topic, sub.Topic);
            Assert.Equal("*", sub.SubString);
            Assert.Equal(0, sub.SubVersion);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 2. Fanned out to each one

    [Fact]
    public void Heartbeat_FansOutToMasterAndSlave()
    {
        using var cluster = MasterSlaveCluster.Start();
        var consumer = new DefaultMQPullConsumer(Group);
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.SetInstanceName("pull-hb-net-fanout");
        consumer.RegisterMessageQueueListener(Topic, new NoopListener());
        consumer.Start();
        try
        {
            Assert.True(WaitFor(() => cluster.CountAt(cluster.MasterAddr, RequestCode.HeartBeat) >= 1
                                      && cluster.CountAt(cluster.SlaveAddr, RequestCode.HeartBeat) >= 1),
                "both the master and the slave receive the heartbeat");
            Assert.Equal(1, consumer.HeartbeatCount);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 3. Registers the group even without a subscription

    [Fact]
    public void Heartbeat_StillAnnouncesTheGroupWithoutRegisterTopics()
    {
        using var cluster = MasterSlaveCluster.Start();
        var consumer = new DefaultMQPullConsumer(Group);
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.SetInstanceName("pull-hb-net-nosub");
        consumer.Start();
        try
        {
            // no registerTopics -> at Start() the route table is empty and that round sends 0 copies; fetch the queues once to fill the route in
            List<MessageQueue> mqs = consumer.FetchSubscribeMessageQueues(Topic);
            Assert.NotEmpty(mqs);
            // both the master and the slave are in the route => one round sends to two (a consumer heartbeat does not skip the slave)
            Assert.Equal(2, consumer.SendHeartbeatToAllBroker());

            Frame hb = cluster.Frames(RequestCode.HeartBeat).First(f => f.Addr == cluster.MasterAddr);
            ConsumerData cd = DecodeFirstConsumerData(hb.Body);
            Assert.Equal(Group, cd.GroupName);
            Assert.Empty(cd.SubscriptionDataSet);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 4. The periodic loop

    [Fact]
    public void HeartbeatLoop_Repeats()
    {
        using var cluster = MasterSlaveCluster.Start();
        var consumer = new DefaultMQPullConsumer(Group);
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.SetInstanceName("pull-hb-net-loop");
        consumer.HeartbeatBrokerIntervalMillis = 300;
        consumer.RegisterMessageQueueListener(Topic, new NoopListener());
        consumer.Start();
        try
        {
            Assert.True(WaitFor(() => consumer.HeartbeatCount >= 3, 6000),
                "the heartbeat loop repeats on the interval, count=" + consumer.HeartbeatCount);

            consumer.HeartbeatEnabled = false;
            long before = consumer.HeartbeatCount;
            Thread.Sleep(900);
            Assert.Equal(before, consumer.HeartbeatCount);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 5. Unregister on shutdown

    [Fact]
    public void Shutdown_UnregistersTheGroupOnEveryBroker()
    {
        using var cluster = MasterSlaveCluster.Start();
        var consumer = new DefaultMQPullConsumer(Group);
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.SetInstanceName("pull-hb-net-unreg");
        consumer.RegisterMessageQueueListener(Topic, new NoopListener());
        consumer.Start();
        Assert.True(WaitFor(() => cluster.CountAt(cluster.MasterAddr, RequestCode.HeartBeat) >= 1));
        consumer.Shutdown();

        Assert.Equal(1, cluster.CountAt(cluster.MasterAddr, RequestCode.UnregisterClient));
        Assert.Equal(1, cluster.CountAt(cluster.SlaveAddr, RequestCode.UnregisterClient));
        Frame unreg = cluster.Frames(RequestCode.UnregisterClient).First();
        Assert.True(unreg.Ext.TryGetValue("consumerGroup", out string? g) && g == Group,
            "35 carries the consumerGroup");
        // the slot that is left empty does not go on the wire (what broker ClientManageProcessor:228/237 checks is group != null)
        Assert.False(unreg.Ext.ContainsKey("producerGroup"), "35 carries no producerGroup slot");

        // idempotent: a repeated Shutdown does not send again
        consumer.Shutdown();
        Assert.Equal(1, cluster.CountAt(cluster.MasterAddr, RequestCode.UnregisterClient));
    }

    // ---------------------------------------------------------------- 6. Lite pull is ACTIVELY too

    [Fact]
    public void LitePull_HeartbeatIsActivelyTyped()
    {
        using var cluster = MasterSlaveCluster.Start();
        var consumer = new DefaultLitePullConsumer(Group);
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.SetInstanceName("lite-hb-net-unit");
        consumer.Subscribe(Topic, "*");
        consumer.Start();
        try
        {
            Assert.True(WaitFor(() => cluster.CountAt(cluster.MasterAddr, RequestCode.HeartBeat) >= 1),
                "the master receives HEART_BEAT after a lite Start()");
            Frame hb = cluster.Frames(RequestCode.HeartBeat).First(f => f.Addr == cluster.MasterAddr);
            ConsumerData cd = DecodeFirstConsumerData(hb.Body);
            Assert.Equal(Group, cd.GroupName);
            Assert.Equal(ConsumeType.ConsumeActively, cd.ConsumeType);
        }
        finally
        {
            consumer.Shutdown();
        }
    }
}
