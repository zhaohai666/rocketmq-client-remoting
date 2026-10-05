// 拉模式 / 轻量拉取消费者的**心跳**离线单测 —— 不需要集群。
//
// 对齐基准（Java 5.5.1，逐行读过；与 python/tests/test_pull_consumer.py、
// cpp/tests/test_pull_consumer_heartbeat.cpp、rust 的同名用例对拍）：
//   * Java DefaultMQPullConsumerImpl.start():746 把本组注册进实例的 consumerTable，
//     实例级周期任务（MQClientInstance#startScheduledTask）再替它逐台发心跳。心跳口径
//     prepareHeartbeatData:1031-1045 = consumeType()（:348 恒 CONSUME_ACTIVELY）+
//     consumeFromWhere()（:353 恒 CONSUME_FROM_LAST_OFFSET）+ messageModel() +
//     subscriptions()（:357-385：逐条 buildSubscriptionData(topic, SUB_ALL) 后
//     **setSubVersion(0L)**）。
//   * Java DefaultLitePullConsumerImpl.consumeType():1111-1112 同样是 CONSUME_ACTIVELY。
//   * shutdown():689-692 = unregisterConsumer → mQClientFactory.shutdown()：每台 broker
//     发 UNREGISTER_CLIENT(35)。
// 为什么值得单测：broker 的 ConsumerManager.consumerTable 按台各一份，缺心跳时
// consumerConnection/38 看不到本组、isRejectPullConsumerEnabled 的 broker 会拒拉
// （PullMessageProcessor:493-505），而拉取本身照样成功 —— 失效是静默的，只能靠抓帧锁死。
//
// 为什么自带假集群：心跳要「打到主从两台」，而 MockCluster 的路由是「一台 broker 一个
// 名字、只有 master」，这条判据在它上面造不出来（与 ProducerUnregisterTests 同一理由）。
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

    /// <summary>一笔上线报文的取证：落在哪台 broker、什么码、extFields / body。</summary>
    private sealed record Frame(string Addr, int Code, PropertyMap Ext, byte[] Body);

    private sealed class NoopListener : IMessageQueueListener
    {
        public void MessageQueueChanged(string topic, IReadOnlyList<MessageQueue> mqAll,
            IReadOnlyList<MessageQueue> mqDivided)
        {
        }
    }

    /// <summary>
    /// 一个只说 remoting 协议的假集群：1 个 namesrv + 同一 brokerName 下的 master(0) 与
    /// slave(1) 两台真监听的 broker。broker 对一切请求回 SUCCESS 空体，全部帧按到达顺序记账。
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
                // 主从地址是运行时才知道的，路由体在这里现拼
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
            // 用真实地址替换 TopicRoute 里的占位名
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

    // ---------------------------------------------------------------- 1. 启动即注册

    [Fact]
    public void Start_AnnouncesTheGroupWithTheJavaShape()
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
                "Start() 后 master 收到 HEART_BEAT");
            Assert.Equal(1, consumer.HeartbeatCount);

            Frame hb = cluster.Frames(RequestCode.HeartBeat).First(f => f.Addr == cluster.MasterAddr);
            ConsumerData cd = DecodeFirstConsumerData(hb.Body);
            Assert.Equal(Group, cd.GroupName);
            // DefaultMQPullConsumerImpl:348 / :353
            Assert.Equal(ConsumeType.ConsumeActively, cd.ConsumeType);
            Assert.Equal(ConsumeFromWhere.ConsumeFromLastOffset, cd.ConsumeFromWhere);
            Assert.Equal(MessageModel.Clustering, cd.MessageModel);
            Assert.True(cd.UnitMode, "unitMode 如实上报（MQClientInstance:1039）");

            // 订阅集来自 registerTopics（subscriptions():357-385）
            SubscriptionData sub = Assert.Single(cd.SubscriptionDataSet);
            Assert.Equal(Topic, sub.Topic);
            Assert.Equal("*", sub.SubString);
            // Java 显式 setSubVersion(0L)：带默认时间戳会让 broker 每轮心跳都以为订阅变了
            Assert.Equal(0, sub.SubVersion);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 2. 扇出到每一台

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
            // Java sendHeartbeatToAllBroker:732-750 遍历每个 brokerId，仅对
            // consumerEmpty && id != MASTER_ID 跳；消费者心跳必带 ConsumerData，故从节点不跳。
            Assert.True(WaitFor(() => cluster.CountAt(cluster.MasterAddr, RequestCode.HeartBeat) >= 1
                                      && cluster.CountAt(cluster.SlaveAddr, RequestCode.HeartBeat) >= 1),
                "主从两台都收到心跳");
            Assert.Equal(1, consumer.HeartbeatCount);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 3. 没有订阅也要注册本组

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
            // 无 registerTopics → Start() 时路由表空、那一轮发 0 份；取一次队列把路由灌进来
            List<MessageQueue> mqs = consumer.FetchSubscribeMessageQueues(Topic);
            Assert.NotEmpty(mqs);
            // 主从两台都在路由里 → 一轮发两台（消费者心跳不跳从节点）
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

    // ---------------------------------------------------------------- 4. 周期循环

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
                "心跳循环按周期重复发送，count=" + consumer.HeartbeatCount);

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

    // ---------------------------------------------------------------- 5. shutdown 注销

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
            "35 带 consumerGroup");
        // 空着的那个槽位不上线（broker ClientManageProcessor:228/237 判的是 group != null）
        Assert.False(unreg.Ext.ContainsKey("producerGroup"), "35 不带 producerGroup 槽位");

        // 幂等：重复 Shutdown 不再发
        consumer.Shutdown();
        Assert.Equal(1, cluster.CountAt(cluster.MasterAddr, RequestCode.UnregisterClient));
    }

    // ---------------------------------------------------------------- 6. 轻量拉取也是 ACTIVELY

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
                "lite Start() 后 master 收到 HEART_BEAT");
            Frame hb = cluster.Frames(RequestCode.HeartBeat).First(f => f.Addr == cluster.MasterAddr);
            ConsumerData cd = DecodeFirstConsumerData(hb.Body);
            Assert.Equal(Group, cd.GroupName);
            // Java DefaultLitePullConsumerImpl.consumeType():1111-1112
            Assert.Equal(ConsumeType.ConsumeActively, cd.ConsumeType);
        }
        finally
        {
            consumer.Shutdown();
        }
    }
}
