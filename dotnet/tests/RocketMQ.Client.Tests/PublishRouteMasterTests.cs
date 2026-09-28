// #100：发布地址查找只认 master（Java MQClientInstance.findBrokerAddressInPublish:1295-1305）的离线单测。
//
// 队列集那一半（topicRouteData2TopicPublishInfo:294-303 跳过没有 master 的 broker）已在
// RouteHeartbeatTests 里锁死；本文件锁的是**地址**侧的同一条分界线：队列挑出来了，发送时还要把
// brokerName 解析成地址，Java 那边是 `brokerAddrTable.get(brokerName).get(MASTER_ID)`（只认主，
// 拿不到返回 null），而不是 `FindBrokerAddressInSubscribe`（主优先、没主退一台从节点）。
//
// 两处都用发布口径，主从切换期间才会是本端立刻报错，而不是把请求打到从节点上再被拒 ——
// 从节点对 SEND_MESSAGE / ACK_MESSAGE / CHANGE_INVISIBLE_TIME 一律 reject
// （SendMessageProcessor:131 ⇒ SYSTEM_BUSY(2)，还是个可重试码），白烧一轮超时，
// 错误类型也和 Java 不一样。
//
// 平表的写入点必须在**真的** `UpdateTopicRouteInfoFromNameServer` 里（Java :962-964），
// 所以这里让实例连一个进程内假 namesrv 走真 RPC，而不是往 `_brokerAddrTable` 里直接塞值 ——
// 绕过它等于这条路径没测。
//
// 与 python/tests/test_publish_route_master.py 的地址侧同题。
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
// PropertyMap 是 src 侧的 global using 别名（SortedDictionary<string,string>），测试项目要显式声明。
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class PublishRouteMasterTests
{
    private const string Broker = "broker-a";
    private const string Topic = "PublishRouteMasterUnitTopic";
    private const long SlaveId = MixAll.MasterId + 1;

    // ---------------------------------------------------------------- 假端点

    /// <summary>
    /// 进程内假端点：既能当 namesrv 回路由，也能当 broker 回 offset 查询。
    /// master 与 slave 各起一个，「请求落在谁身上」直接从 socket 上取证。
    /// </summary>
    private sealed class RouteEndpoint : IDisposable
    {
        /// <summary>四个查询各自的应答值（互不相同，证明取回的是这一路的数据）。</summary>
        public const long MaxOffsetReply = 7;
        public const long MinOffsetReply = 3;
        public const long SearchOffsetReply = 5;
        public const long EarliestReply = 1700000000000;

        private readonly object _gate = new();
        private readonly Dictionary<string, byte[]> _routes = new();
        private readonly List<(int Code, PropertyMap Ext)> _requests = new();
        private readonly Socket _listener;

        public string Addr { get; }

        /// <summary>QUERY_CONSUMER_OFFSET(14) 的应答位点（主/从各设一个互不相同的值）。</summary>
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

        /// <summary>四个 offset 查询（29/30/31/32）一共收到多少笔 —— 「有没有 wire 打到这台」。</summary>
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
                // 把请求体的 mqSet 原样回成 lockOKMQSet：返回的锁集非空即证明「这一发真的落地了」
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
                // 长轮询空手而归是常态：回 210 POLLING_TIMEOUT，客户端按 PollingNotFound 收
                return Echo(req, ResponseCode.PollingTimeout, "mock: no message");
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

    // ---------------------------------------------------------------- 夹具

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

    /// <summary>建实例并把 Topic 路由塞进缓存（真 RPC 走一趟假 namesrv）。</summary>
    private static MQClientInstance NewInstanceWithRoute(RouteEndpoint namesrv, string clientId)
    {
        var instance = new MQClientInstance(clientId, new List<string> { namesrv.Addr });
        Assert.True(instance.UpdateTopicRouteInfoFromNameServer(Topic), "路由要真的进缓存");
        return instance;
    }

    /// <summary>本端报的「broker 不存在」：消息是 Java 原文，码是 -1（不是 broker 回的码）。</summary>
    private static void AssertNotExist(MQClientException e)
    {
        Assert.Equal("The broker[" + Broker + "] not exist", e.Message);
        Assert.Equal(-1, e.ResponseCode);
    }

    // ---------------------------------------------------------------- 地址解析

    /// <summary>
    /// <c>FindBrokerAddressInPublish</c> 只认 brokerId=0；同一张路由上「主优先、没主退一台」
    /// 的口径（<c>BrokerAddrOf</c>，给心跳/拉取用）必须拿得到从节点 —— 两条口径不能合成一条。
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
            // 停后形状：平表里 broker-a 只剩 brokerId=1
            Assert.Equal(string.Empty, instance.FindBrokerAddressInPublish(Broker));
            // 负控：退让口径（心跳/拉取/位点查询用）在**同一张路由**上必须拿得到从节点地址
            Assert.Equal(slave.Addr, instance.BrokerAddrOf(Broker));

            // master 重新注册：发布口径立刻解析得出（跳的是「没有 master」，不是 broker-a 这个名字）
            namesrv.AddRoute(Topic, RouteOf(Broker, (MixAll.MasterId, master.Addr), (SlaveId, slave.Addr)));
            Assert.True(instance.UpdateTopicRouteInfoFromNameServer(Topic));
            Assert.Equal(master.Addr, instance.FindBrokerAddressInPublish(Broker));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    /// <summary>Java <c>sendKernelImpl:919-924</c>：发布地址查不到，按 topic 刷一次路由再查。</summary>
    [Fact]
    public void PublishAddrForRefreshesTheRouteThenRechecks()
    {
        using RouteEndpoint namesrv = RouteEndpoint.Start();
        using RouteEndpoint master = RouteEndpoint.Start();
        var instance = new MQClientInstance("prm-refresh@1", new List<string> { namesrv.Addr });
        try
        {
            // 还没见过这个 topic：那次「查不到 → 刷路由 → 重查」必须真的发生
            namesrv.AddRoute(Topic, RouteOf(Broker, (MixAll.MasterId, master.Addr)));
            Assert.Equal(master.Addr, instance.PublishAddrFor(Broker, Topic));
            Assert.Equal(1, namesrv.CountRequests(RequestCode.GetRouteinfoByTopic));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    /// <summary>主掉线（路由里只剩 brokerId=1）：本端报「broker 不存在」，一次 broker 请求都不发。</summary>
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
            // 报错前必须先刷一次路由（Java 的 tryToFindTopicPublishInfo），不能直接拿旧结论结账
            Assert.Equal(before + 1, namesrv.CountRequests(RequestCode.GetRouteinfoByTopic));
            // 一次 broker wire 都没发：从节点上是「发过去再被拒」，这里压根不发
            Assert.Equal(0, slave.CountOffsetQueries());

            // 负控：跳的是「没有 master」，不是 broker-a 这个名字 —— master 一注册立刻解析得出
            namesrv.AddRoute(Topic, RouteOf(Broker, (MixAll.MasterId, master.Addr), (SlaveId, slave.Addr)));
            Assert.Equal(master.Addr, instance.PublishAddrFor(Broker, Topic));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    /// <summary>路由里压根没有这个 brokerName（拼错/已下线）：同样报 not exist，不退到别的 broker。</summary>
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
            // 负控：平表确实写进去了（broker-b 查得到），缺的只是 broker-a 这一条
            Assert.Equal(other.Addr, instance.FindBrokerAddressInPublish("broker-b"));
        }
        finally
        {
            instance.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 管理端 offset 查询

    /// <summary>
    /// <c>MQAdminImpl:195/214/232/250</c> 的四个 offset 查询同一口径：只打主，主没了就报
    /// not exist、一条 wire 都不发（旧行为是退到从节点上把查询做完，静默给出「主的数据」）。
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

            // 负控：主回来之后四路都查得到，且请求落在**主**地址上（不是路由里那台从节点）
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

    // ---------------------------------------------------------------- 订阅口径（#104）

    /// <summary>
    /// Java <c>RebalanceImpl#lock:153 / lockAll:195</c>（解锁 <c>#unlock:74 / unlockAll:104</c>）：
    /// 队列锁的地址是 <c>findBrokerAddressInSubscribe(brokerName, MASTER_ID, true)</c> ——
    /// 只认主、**不刷路由**，拿不到就整台跳过。路由里只剩从节点时 LOCK/UNLOCK 一条都不该上线：
    /// 从节点上锁等于锁在它自己的锁管理器里，master 不知情，顺序消费的互斥保证静默失效。
    /// </summary>
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
            // 也不许刷路由：Java 的 lock/unlock 直接 findBrokerAddressInSubscribe(false) 收场，
            // 没有发送路径上的那一次 tryToFindTopicPublishInfo
            Assert.Equal(before, namesrv.CountRequests(RequestCode.GetRouteinfoByTopic));

            // 负控：主回来之后锁/解锁都落在主地址上、返回的锁集就是请求的那两个队列
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

    /// <summary>
    /// Java <c>PullAPIWrapper#popAsync:369-373</c>：POP 的地址同一条订阅口径（只认主）——
    /// 查不到按 topic 刷一次路由再查，仍查不到在本端报「The broker[X] not exist」，
    /// 一条 POP wire 都不发（从节点收 POP 只会换回一个可重试的 SYSTEM_BUSY）。
    /// </summary>
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
            // 报错前必须刷过一次路由（popAsync 的 findBrokerAddressInSubscribe + 重查）
            Assert.Equal(before + 1, namesrv.CountRequests(RequestCode.GetRouteinfoByTopic));
            Assert.Equal(0, slave.CountRequests(RequestCode.PopMessage));

            // 负控：主注册后（这一发会顺带触发一次路由刷新）请求落在主地址上，从节点一条没有
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

    /// <summary>
    /// Java <c>RemoteBrokerOffsetStore#fetchConsumeOffsetFromBroker:237-241</c>：位点查询先只认主，
    /// 查不到按 topic 刷一次路由，重查时**放宽到从节点**（位点是 HA 复制来的同一份数据，Java 允许
    /// 从从节点读），仍没有才抛「The broker[X] not exist」。与管理侧 offset 查询（一律打主）
    /// 的差别只在这最后一步。
    /// </summary>
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

            // 主没了：退到从节点上把查询做完，取回的是**从节点**那份值
            Assert.True(instance.QueryConsumerOffset("GID_prm_offset", mq, out long offset));
            Assert.Equal(424242L, offset);
            Assert.Equal(1, slave.CountRequests(RequestCode.QueryConsumerOffset));
            Assert.Equal(before + 1, namesrv.CountRequests(RequestCode.GetRouteinfoByTopic));

            // 负控：主回来之后只打主（从节点计数不再增长）
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

        // 路由里压根没有 broker-a（连从节点都没有）：刷一次路由后仍查不到 ⇒ 本端报 not exist
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
