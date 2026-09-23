// 生产者/消费者退出时发 UNREGISTER_CLIENT(35) 的离线单测 —— 不需要集群。
//
// 对齐基准（Java 5.5.1，逐行读过；与 python/tests/test_producer_unregister.py、
// cpp/tests/test_producer_unregister.cpp、rust/src/client/mq_client.rs 的同一组断言对拍）：
//   * `DefaultMQProducerImpl#shutdown`:313 → `MQClientInstance#unregisterProducer`:1198-1201
//     → 私有 `unregisterClient(producerGroup, null)`:1158-1182 —— 遍历 `brokerAddrTable` 的
//     **每个 brokerId**（主 + 从），每台一发，超时 `getMqClientApiTimeout()`（3000ms），
//     RemotingException / InterruptedException / MQBrokerException 一律吞成 log.warn。
//   * `MQClientAPIImpl#unregisterClient`:1615-1639 —— 头是
//     `UnregisterClientRequestHeader{clientID, producerGroup, consumerGroup}`，键名是
//     大写 ID 的 `clientID`；没用到的那个槽位传 null ⇒ **整个字段不上线**。
//   * broker 端 `ClientManageProcessor#unregisterClient`:213-249 判的是 `group != null`：
//     空串会被当成「真有个空组名」去查 `""` 的订阅组配置。这类偏差真机不会报错，
//     只能靠抓帧锁住。
//   * 心跳一侧仍只打「主优先」的那台：`GetRouteOfAllBrokers`（`SelectBrokerAddr`）与
//     `GetAllBrokerAddrs` 的分工必须守住，这里一起锁。
//
// 为什么这里要自带一个假集群：`MockCluster` 的路由是「一台 broker 一个名字、只有 master」，
// 本机真集群也只有一台 master，**从节点收不收得到 35 这一条判据根本造不出来**。
// 下面 [`MasterSlaveCluster`] 刻意在同一 brokerName 下挂 master(0) + slave(1)。
// 与 examples 的 `unreg-live`（真机 204 前后对照）互补：真机证明「broker 确实摘了」，
// 这里证明「线上走了什么形状、打给了哪几台」。
using System.Buffers.Binary;
using System.Globalization;
using System.Net;
using System.Net.Sockets;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
// PropertyMap 是 src 侧的 global using 别名（SortedDictionary<string,string>），测试项目要显式声明。
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

using Xunit;

namespace RocketMQ.Client.Tests;

public class ProducerUnregisterTests
{
    private const string Topic = "UnregNetUnitTopic";
    private const string Group = "PID_unreg_net_unit";
    private const string ClientId = "10.0.0.1@unreg-net-unit";

    /// <summary>35 在离线用例里用的超时 —— Java 的 mqClientApiTimeout，写死防止被改回 5000。</summary>
    private const int JavaApiTimeout = 3000;

    // ---------------- master + slave 假集群 ----------------

    /// <summary>一笔 35（或任意请求）的取证：落在哪台 broker、什么码、extFields 长什么样。</summary>
    private sealed record Frame(string Addr, int Code, PropertyMap Ext);

    /// <summary>
    /// 一个只说 remoting 协议的假集群：1 个 namesrv + 同一 brokerName 下的 master(0) 与
    /// slave(1) 两台真监听的 broker。路由由 <see cref="TopicRoute"/> 手工拼出来，
    /// 因为「注销必须打到从节点」这条判据在真机和 MockCluster 上都不可达。
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
                // 同一 brokerName 下 master(brokerId=0) + slave(brokerId=1)：
                // Java `MQClientInstance#unregisterClient`:1159-1166 遍历的正是这层的每个条目。
                new SortedDictionary<long, string> { { 0L, MasterAddr }, { 1L, SlaveAddr } }));
            return route;
        }

        /// <summary>让某台 broker 从此把（任何）请求都回成指定应答码，用来造单台失败。</summary>
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
                _frames.Add(new Frame(addr, req.Code, new PropertyMap(req.ExtFields)));
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
                    // 关监听端口时不需要处理任何异常
                }
            }
        }
    }

    // ---------------- 帧收发（与 MockCluster 同一套最小实现） ----------------

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
                    return; // Dispose 之后 Accept 必然抛，正常收工
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
                            // 半路断连、解码失败：丢掉这条连接即可
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

    /// <summary>指向假集群、路由已缓存好的实例（用例结束顺手关掉连接）。</summary>
    private static MQClientInstance StartedInstance(MasterSlaveCluster cluster)
    {
        var inst = new MQClientInstance(ClientId, new[] { cluster.NamesrvAddr });
        Assert.NotNull(inst.GetTopicRouteData(Topic));
        return inst;
    }

    // ---------------- 1~3：头的形状 ----------------
    [Fact]
    public void ProducerSideCarriesOnlyClientIdAndProducerGroup()
    {
        using MasterSlaveCluster cluster = MasterSlaveCluster.Start();
        using MQClientInstance inst = StartedInstance(cluster);

        inst.UnregisterClient(cluster.MasterAddr, ClientId, Group, "", JavaApiTimeout);

        List<Frame> got = cluster.Frames(RequestCode.UnregisterClient);
        Assert.Single(got);
        PropertyMap ext = got[0].Ext;
        // 键名 clientID 的 D 是大写（Java 的 @CFHeader 字段名），拼错 broker 直接读不到
        Assert.Equal(ClientId, ext["clientID"]);
        Assert.Equal(Group, ext["producerGroup"]);
        // Java 的 unregisterClient(group, null)：消费者槽位整个不上线
        Assert.False(ext.ContainsKey("consumerGroup"));
        Assert.Equal(2, ext.Count);
    }

    [Fact]
    public void ConsumerSideCarriesOnlyClientIdAndConsumerGroup()
    {
        using MasterSlaveCluster cluster = MasterSlaveCluster.Start();
        using MQClientInstance inst = StartedInstance(cluster);

        inst.UnregisterClient(cluster.MasterAddr, ClientId, "", Group, JavaApiTimeout);

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

        inst.UnregisterClient(cluster.MasterAddr, ClientId, "PID_a", "GID_b", JavaApiTimeout);

        PropertyMap ext = Assert.Single(cluster.Frames(RequestCode.UnregisterClient)).Ext;
        // PropertyMap 是 SortedDictionary ⇒ 这里锁的是「三个键都在」，顺序按字典序
        Assert.Equal(3, ext.Count);
        Assert.Equal("PID_a", ext["producerGroup"]);
        Assert.Equal("GID_b", ext["consumerGroup"]);
    }

    [Fact]
    public void WhitespaceGroupIsTreatedAsAbsent()
    {
        using MasterSlaveCluster cluster = MasterSlaveCluster.Start();
        using MQClientInstance inst = StartedInstance(cluster);

        // 纯空白与空串同处理：合法组名不可能全是空白（Validators 那一关过不去），
        // 传进来只可能是调用方漏了值。broker 判的是 `group != null`，所以必须整个不上线。
        inst.UnregisterClient(cluster.MasterAddr, ClientId, "   ", Group, JavaApiTimeout);

        PropertyMap ext = Assert.Single(cluster.Frames(RequestCode.UnregisterClient)).Ext;
        Assert.False(ext.ContainsKey("producerGroup"));
        Assert.Equal(Group, ext["consumerGroup"]);
        Assert.Equal(2, ext.Count);
    }

    // ---------------- 4：扇出含从节点 ----------------

    [Fact]
    public void FanOutReachesSlaveToo_WhileHeartbeatHelperStaysMasterOnly()
    {
        using MasterSlaveCluster cluster = MasterSlaveCluster.Start();
        using MQClientInstance inst = StartedInstance(cluster);

        // 分工：心跳/「问到一台就行」的路径只打 master 优先的那台……
        Assert.Equal(new[] { cluster.MasterAddr }, inst.GetRouteOfAllBrokers());
        // ……注销必须每台各一发（含 slave），Java :1159-1166 遍历的是每个 brokerId。
        Assert.Equal(new[] { cluster.MasterAddr, cluster.SlaveAddr }.OrderBy(a => a, StringComparer.Ordinal)
                     .ToList(),
            inst.GetAllBrokerAddrs().OrderBy(a => a, StringComparer.Ordinal).ToList());

        inst.UnregisterClientAllBrokers(ClientId, Group, "", JavaApiTimeout);

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

    [Fact]
    public void OneBrokerFailingDoesNotAbortTheFanOut()
    {
        using MasterSlaveCluster cluster = MasterSlaveCluster.Start();
        using MQClientInstance inst = StartedInstance(cluster);
        cluster.FailFrom(cluster.SlaveAddr, ResponseCode.SystemError);

        // 底层那一发**要**抛（带上 broker 的响应码），否则下面的「吞」就成了「什么都没发」
        MQBrokerException ex = Assert.Throws<MQBrokerException>(() =>
            inst.UnregisterClient(cluster.SlaveAddr, ClientId, Group, "", JavaApiTimeout));
        Assert.Equal(ResponseCode.SystemError, ex.ResponseCode);

        cluster.ClearFrames();
        // Java :1172-1178 三种异常全部只 log.warn —— shutdown 不因单台抖动中断，
        // 而且剩下的 broker 照样要注销到。
        inst.UnregisterClientAllBrokers(ClientId, Group, "", JavaApiTimeout);
        Assert.Equal(1, cluster.CountAt(cluster.MasterAddr, RequestCode.UnregisterClient));
        Assert.Equal(1, cluster.CountAt(cluster.SlaveAddr, RequestCode.UnregisterClient));
    }

    // ---------------- 5：超时预算与 Java 对齐 ----------------

    [Fact]
    public void UnregisterTimeoutMatchesJavaApiTimeout()
    {
        Assert.Equal(3000, MQClientInstance.MqClientApiTimeoutMillis);
    }

    // ---------------- 6：生产者 shutdown 真的发出这一发，且排在发送之后 ----------------

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

        // 两台 broker（路由里各自的 master）各一发
        Assert.Equal(2, cluster.CountRequests(RequestCode.UnregisterClient));
        Assert.Equal(2, cluster.CountRequestsWith(RequestCode.UnregisterClient,
            "producerGroup", p.ProducerGroup));
        Assert.False(cluster.AnyRequestHas(RequestCode.UnregisterClient, "consumerGroup"));

        // 35 必须排在业务发送之后：它走的是**还没关的那条长连接**。
        List<WireRecord> records = cluster.Records();
        int lastSend = records.FindLastIndex(x =>
            x.Code is RequestCode.SendMessage or RequestCode.SendMessageV2);
        int firstUnreg = records.FindIndex(x => x.Code == RequestCode.UnregisterClient);
        Assert.True(lastSend >= 0 && firstUnreg > lastSend,
            "last_send=" + lastSend.ToString(CultureInfo.InvariantCulture)
            + " first_unreg=" + firstUnreg.ToString(CultureInfo.InvariantCulture)
            + " records=" + records.Count.ToString(CultureInfo.InvariantCulture));
        // 注销的是**本 clientId**（不是别的组冒名），值必须与生产者算出来的一模一样
        Assert.Equal(p.ClientId, records[firstUnreg].Ext.GetValueOrDefault("clientID"));
    }

    [Fact]
    public void DeadBrokerInRouteDoesNotBreakProducerShutdown()
    {
        // 路由里掺一台连不上的 broker：注销它必然抛连接异常，Java 吞掉并继续下一台，
        // 整个 shutdown 不能被打断。
        using MockCluster cluster = MockCluster.StartAt(
            new List<string?> { null, MockCluster.DeadAddr() });
        var p = new DefaultMQProducer(Group + "_dead") { NamesrvAddr = cluster.NamesrvAddr };
        p.Start();
        // 只往活着的 broker-0 发，避免发送本身撞死地址
        p.Send(new Message(Topic, System.Text.Encoding.UTF8.GetBytes("hi")),
            new MessageQueue(Topic, MockCluster.BrokerName(0), 0), 5000);

        p.Shutdown();

        Assert.Equal(1, cluster.CountRequestsWith(RequestCode.UnregisterClient,
            "producerGroup", p.ProducerGroup));
    }

}
