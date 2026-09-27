// P5：postSubscriptionWhenPull + updatePullFromWhichNode（PullAPIWrapper 移植）的离线单测。
//
// 对齐基准（Java 5.5.0，逐行读过）：
//   * DefaultMQPushConsumer#postSubscriptionWhenPull 默认 false；
//     DefaultMQPushConsumerImpl.pullMessage:458-468 里
//     `subExpression = (postSubscriptionWhenPull && !sd.isClassFilterMode()) ? sd.getSubString() : null`，
//     sysFlag 的 SUBSCRIPTION 位 = `subExpression != null`。默认关闭是安全的：tag 过滤由客户端
//     FilterMessagesForDelivery 兜底。
//   * PullAPIWrapper#pullKernelImpl:197-205 用 recalculatePullFromWhichNode(mq) 调
//     MQClientInstance#findBrokerAddressInSubscribe:1307-1336；命中从节点时
//     PullSysFlag.clearCommitOffsetFlag(:219-221)。
//   * PullAPIWrapper#processPullResult:77 用应答头的 suggestWhichBrokerId 回写
//     pullFromWhichNodeTable（:157-164 的 updatePullFromWhichNode）。
//
// 与 python/tests/test_pull_post_subscription.py 同题；报文形状（SUBSCRIPTION 位、
// `subscription` 键是否上线、打给 master 还是 slave、COMMIT_OFFSET 清位）在这里用
// 进程内假端点从 socket 上取证，不靠调用方自说自话。
using System.Buffers.Binary;
using System.Net;
using System.Net.Sockets;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;
// PropertyMap 是 src 侧的 global using 别名（SortedDictionary<string,string>），测试项目要显式声明。
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class PullPostSubscriptionTests
{
    private const string Group = "GID_P5Unit";
    private const string Topic = "P5PullTopic";
    private const string Broker = "broker-a";
    private const string ClientId = "127.0.0.1@5555#1";
    private const int MaxFrame = 20 * 1024 * 1024;

    // ---------------------------------------------------------------- 假端点

    /// <summary>
    /// 进程内假端点：既能当 namesrv 回路由，也能当 broker 回 PULL_MESSAGE。
    /// master 与 slave 各起一个，于是「打给了谁」直接从 socket 上取证。
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

        /// <summary>脚本化 PULL_MESSAGE 应答头（nextBeginOffset / maxOffset / suggestWhichBrokerId…）。</summary>
        public void ScriptPullResponse(PropertyMap ext)
        {
            lock (_gate)
            {
                _pullExt = ext;
                _pulls.Clear();
            }
        }

        /// <summary>收到的 PULL_MESSAGE 的请求头快照（上线原样）。</summary>
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
                // 关监听端口不需要处理异常
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
                    return;  // Dispose 之后 Accept 必抛，正常收工
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
                            // 半路断连 / 解码失败：丢掉这条连接即可
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
                    continue;  // oneway 没有应答可写
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

            // 心跳之类一律成功，别让后台线程卡在错误上
            return req.IsOnewayRpc() ? null : Echo(req, ResponseCode.Success, null);
        }

        private static RemotingCommand Echo(RemotingCommand req, int code, string? remark)
        {
            var resp = RemotingCommand.CreateResponseCommand(code, remark);
            // 客户端按 opaque 配对，串了的响应会被当噪声丢掉
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

    /// <summary>一个已关掉、必然拒连的回环端口（构造「从节点不存在」用）。</summary>
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

    /// <summary>
    /// Java <c>findBrokerAddressInSubscribe:1307-1336</c> 的四个分支：命中直接用；
    /// 从节点没命中按 id+1 再试；都不命中且不限定时回退到 id 最小那台。`IsSlave` 一律按
    /// **命中的 id** 判，不是传进来的 brokerId。
    /// </summary>
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

        // 从节点 id 缺失：按 id+1 再试（Java 的从节点编号约定）
        var sparse = new SortedDictionary<long, string>
        {
            [0] = "127.0.0.1:10911",
            [2] = "127.0.0.1:10931",
        };
        Assert.Equal(("127.0.0.1:10931", true),
            MQClientInstance.FindBrokerAddressInSubscribe(sparse, 1));

        // 都不命中且不限定时回退到 id 最小的（Java 取 map 首项，这里取确定性形态）
        Assert.Equal(("127.0.0.1:10911", false),
            MQClientInstance.FindBrokerAddressInSubscribe(addrs, 9));
        Assert.Equal(("127.0.0.1:10921", true),
            MQClientInstance.FindBrokerAddressInSubscribe(
                new SortedDictionary<long, string> { [1] = "127.0.0.1:10921" }, 3));

        // onlyThisBroker：宁缺毋滥
        Assert.Equal((string.Empty, false),
            MQClientInstance.FindBrokerAddressInSubscribe(addrs, 9, onlyThisBroker: true));
        Assert.Equal((string.Empty, false),
            MQClientInstance.FindBrokerAddressInSubscribe(
                new SortedDictionary<long, string>(), 0));
    }

    // ---------------------------------------------------------------- 线上报文

    /// <summary>
    /// brokerId=1（从节点）：请求打到 slave 地址；COMMIT_OFFSET 位被清掉
    /// （Java pullKernelImpl:219-221）；SUBSCRIPTION 位关闭时 `subscription` 键整个不上线
    /// （Java 的 subExpression=null + makeCustomHeaderToNet 跳过 null）。
    /// </summary>
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
            Assert.False(PullSysFlag.HasCommitOffsetFlag(sentFlag), "slave 上一次 COMMIT_OFFSET 都没有意义");
            Assert.True(PullSysFlag.HasSuspendFlag(sentFlag), "suspend 位保持");
            Assert.False(sent.ContainsKey("subscription"),
                "SUBSCRIPTION 位关闭时该字段根本不进 extFields（Java 的 null → 丢字段）");
            // 应答头 suggestWhichBrokerId 透传给调用方，供回写 pullFromWhichNodeTable
            Assert.Equal(0L, result.SuggestWhichBrokerId);
        }
        finally
        {
            instance.Shutdown();
        }
    }

    /// <summary>
    /// brokerId=0（主节点）：请求打到 master、COMMIT_OFFSET 位保留；SUBSCRIPTION 位置位时
    /// `subscription` 才上线，内容是调用方给的表达式；老 broker 不带 suggestWhichBrokerId
    /// 时透传 null（调用方按 master=0 记账）。
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
            Assert.True(PullSysFlag.HasCommitOffsetFlag(sentFlag), "master 上位点照提交");
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

    /// <summary>
    /// 从节点不存在（brokerId=1 但路由里只有 master）：Java 的
    /// `findBrokerAddressInSubscribe` 在 onlyThisBroker=false 时会**回退到主节点**，
    /// 所以这里必须打到 master 而不是抛异常。注意 `IsSlave` 按命中的 id 判，于是
    /// COMMIT_OFFSET 位**保留**（回退到的是 master）。
    /// </summary>
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
                "回退到的是 master，位点照提交（IsSlave 按命中的 id 判，不是请求的 brokerId）");
        }
        finally
        {
            instance.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 消费者配置与记账

    /// <summary>
    /// Java `postSubscriptionWhenPull` 默认 false：默认**不上送**订阅表达式；打开后且非类过滤
    /// 模式才上送（空表达式归一成 "*"）。
    /// </summary>
    [Fact]
    public void ConsumerSubscriptionGatingFollowsJavaDefault()
    {
        var consumer = new DefaultMQPushConsumer(Group);
        var sub = new SubscriptionData(Topic, "TagA");
        Assert.False(consumer.PostSubscriptionWhenPull);  // Java 5.x 默认 false
        Assert.Null(consumer.PullSubscriptionExpressionForTest(sub));

        consumer.PostSubscriptionWhenPull = true;
        Assert.Equal("TagA", consumer.PullSubscriptionExpressionForTest(sub));

        var blank = new SubscriptionData(Topic, "");
        Assert.Equal("*", consumer.PullSubscriptionExpressionForTest(blank));

        // 类过滤模式：表达式是过滤类名，broker 侧 TAG 过滤会误判，所以即使打开也不上送
        var classMode = new SubscriptionData(Topic, "com.example.MyFilter") { ClassFilterMode = true };
        Assert.Null(consumer.PullSubscriptionExpressionForTest(classMode));
    }

    /// <summary>
    /// Java `PullAPIWrapper#recalculatePullFromWhichNode` / `#updatePullFromWhichNode`：
    /// 首轮无记录按 master=0；应答缺 suggestWhichBrokerId 也按 0 记账（Java long 原语，
    /// 不是「保留旧值」）；表按队列 key 隔离。
    /// </summary>
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

    // ---------------------------------------------------------------- 夹具

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

    /// <summary>建实例并把 Topic 路由塞进缓存（真 RPC 走一趟 namesrv）。</summary>
    private static MQClientInstance NewInstanceWithRoute(PullEndpoint namesrv)
    {
        var instance = new MQClientInstance(ClientId, new List<string> { namesrv.Addr });
        Assert.True(instance.UpdateTopicRouteInfoFromNameServer(Topic), "路由要真的进缓存");
        return instance;
    }

    /// <summary>死端口那条路径的守卫（`FindBrokerAddressInSubscribe` 不该把死地址当命中）。</summary>
    [Fact]
    public void DeadPortIsStillReturnedAsTheHit()
    {
        string dead = DeadAddr();
        var addrs = new SortedDictionary<long, string> { [MixAll.MasterId] = dead };
        // 命中与否只按表内容判，连通性是调用方的事（Java 亦然）
        Assert.Equal((dead, false), MQClientInstance.FindBrokerAddressInSubscribe(addrs, 0));
    }
}
