// FIRST_OFFSET 的起点语义（Java RebalancePushImpl:197-208 / RebalanceLitePullImpl:114-124）
// 离线单测：起点是**字面量 0**，一个 GET_MIN_OFFSET（31）都不发。
//
// 为什么必须离线锁死：这条分支原来是 `getMinOffset(mq)`，而 minOffset 属 MQAdminImpl 口径
// ——只认 master（findBrokerAddressInPublish）。主节点掉线期间，新起的 FIRST_OFFSET 消费者
// 会当场抛「The broker[X] not exist」，一条都拉不到；而 Java 的起点是字面量 0
// （上游原话：`//the offset will be fixed by the OFFSET_ILLEGAL process`），从节点本来就能
// 按 0 起拉，越界由 broker 用 `PULL_OFFSET_MOVED` 把位点纠回来（客户端的 OFFSET_ILLEGAL 分支）。
// 多打这一枪的代价不只是慢：真机上没有任何一条断言会因此变红，只有"主挂着的窗口里消费者起不来"
// 这一个现象——所以锁在离线的报文层。
//
// 报文层面不取巧：进程内假 namesrv/broker 在**真 socket** 上回路由、回"没有已提交位点"、
// 回拉取应答；「这个 RPC 到底发没发」全部从 socket 上取证。负控腿（LAST_OFFSET 那一支必须
// 发 GET_MAX_OFFSET）证明这份请求日志不是哑的。
//
// 与 rust/src/client/pull_consumer.rs 的 `lite_first_offset_starts_at_zero_without_a_min_offset_rpc`
// 同题；push 那条腿在这里多补一份（订阅模式走的是 Consumer.cs 的 ResolveInitialOffset）。
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

public class InitialOffsetTests
{
    private const string Topic = "InitialOffsetTopic";
    private const string Broker = "broker-a";
    private const int MaxFrame = 20 * 1024 * 1024;

    private static MessageQueue Queue0() => new(Topic, Broker, 0);

    // ---------------------------------------------------------------- 假端点

    /// <summary>一笔到达的请求：码 + extFields 快照。</summary>
    private sealed record WireReq(int Code, PropertyMap Ext);

    /// <summary>
    /// 假 namesrv + 假 broker（同一具壳）：回路由、回"这位点没提交过"、回拉取应答，
    /// 并把每一笔请求按到达顺序记账。GET_MIN_OFFSET / GET_MAX_OFFSET **照常应答**
    /// —— 应答了才谈得上"客户端自己选择不发"，否则测的是"发了也没人理"。
    /// </summary>
    private sealed class OffsetEndpoint : IDisposable
    {
        private readonly object _gate = new();
        private readonly List<WireReq> _reqs = new();
        private readonly Socket _listener;
        /// <summary>从心跳（34）body 里认下的 clientID。rebalance 要靠它把自己排进 cidAll，
        /// 否则这台消费者一个队列都分不到、永远不会发拉取（pull 腿就白测了）。</summary>
        private string _clientId = string.Empty;

        public string Addr { get; }

        private OffsetEndpoint(Socket listener)
        {
            _listener = listener;
            Addr = ((IPEndPoint)listener.LocalEndPoint!).ToString();
            new Thread(AcceptLoop) { IsBackground = true }.Start();
        }

        public static OffsetEndpoint Start()
        {
            var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            server.Listen(16);
            return new OffsetEndpoint(server);
        }

        public List<WireReq> Requests(int code)
        {
            lock (_gate)
            {
                return _reqs.Where(r => r.Code == code).ToList();
            }
        }

        /// <summary>按到达顺序的全部请求码 —— 失败信息里带一份，省得为一个假端点再起一次调试。</summary>
        public List<int> Codes()
        {
            lock (_gate)
            {
                return _reqs.Select(r => r.Code).ToList();
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
            lock (_gate)
            {
                _reqs.Add(new WireReq(req.Code, new PropertyMap(req.ExtFields)));
            }

            if (req.Code == RequestCode.HeartBeat && req.Body.Length > 0)
            {
                // 心跳 body 里的 clientID 就是这台消费者的身份：rebalance 拿它跟 cidAll 对齐
                if (HeartbeatData.Decode(req.Body, out HeartbeatData hb) && hb.ClientId.Length > 0)
                {
                    lock (_gate)
                    {
                        _clientId = hb.ClientId;
                    }
                }
            }

            if (req.Code == RequestCode.GetRouteinfoByTopic)
            {
                string topic = req.ExtFields.TryGetValue("topic", out string? t) ? t : "";
                if (topic != Topic)
                {
                    return Echo(req, ResponseCode.TopicNotExist, "mock: no route");
                }

                RemotingCommand routeResp = Echo(req, ResponseCode.Success, null);
                routeResp.Body = BuildRoute(Addr);
                routeResp.HasBody = true;
                return routeResp;
            }

            if (req.Code == RequestCode.GetConsumerListByGroup && !req.IsOnewayRpc())
            {
                // rebalance 的第一步：这台消费者必须在名单里，否则分不到队列、也就不会拉取
                string clientId;
                lock (_gate)
                {
                    clientId = _clientId;
                }

                if (clientId.Length == 0)
                {
                    return Echo(req, ResponseCode.Success, null);
                }

                var body = new GetConsumerListByGroupResponseBody();
                body.ConsumerIdList.Add(clientId);
                RemotingCommand listResp = Echo(req, ResponseCode.Success, null);
                listResp.Body = body.Encode();
                listResp.HasBody = true;
                return listResp;
            }

            if (req.Code == RequestCode.QueryConsumerOffset && !req.IsOnewayRpc())
            {
                // 新组：broker 上没有已提交位点 —— 正是 FIRST_OFFSET 分支要处理的局面
                return Echo(req, ResponseCode.QueryNotFound, "mock: no committed offset");
            }

            if (req.Code is RequestCode.GetMaxOffset or RequestCode.GetMinOffset
                && !req.IsOnewayRpc())
            {
                RemotingCommand offsetResp = Echo(req, ResponseCode.Success, null);
                offsetResp.AddExtField("offset", "5");
                return offsetResp;
            }

            if (req.Code == RequestCode.PullMessage && !req.IsOnewayRpc())
            {
                // 立刻回"这轮没有新消息"：断言只看请求头里的 queueOffset，不看应答语义。
                RemotingCommand pullResp = Echo(req, ResponseCode.PullNotFound, null);
                pullResp.AddExtField("nextBeginOffset", "0");
                pullResp.AddExtField("minOffset", "0");
                pullResp.AddExtField("maxOffset", "0");
                return pullResp;
            }

            // 心跳 / 提交位点之类一律成功，别让后台线程卡在错误上
            return req.IsOnewayRpc() ? null : Echo(req, ResponseCode.Success, null);
        }

        private static byte[] BuildRoute(string brokerAddr)
        {
            var route = new TopicRouteData();
            route.QueueDatas.Add(new QueueData(Broker, 1, 1, PermName.PermRead | PermName.PermWrite, 0));
            route.BrokerDatas.Add(new BrokerData("DefaultCluster", Broker,
                new SortedDictionary<long, string> { { MixAll.MasterId, brokerAddr } }));
            return route.Encode();
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

    private static bool WaitFor(Func<bool> cond, TimeSpan timeout)
    {
        DateTime deadline = DateTime.UtcNow + timeout;
        while (DateTime.UtcNow < deadline)
        {
            if (cond()) return true;
            Thread.Sleep(20);
        }

        return cond();
    }

    private sealed class NullListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
                                                       ConsumeConcurrentlyContext context)
            => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }

    // ---------------------------------------------------------------- 用例

    /// <summary>
    /// 轻量拉取：FIRST_OFFSET 的起点是字面量 0，第一笔拉取就带着 queueOffset=0 上线，
    /// 全程一个 GET_MIN_OFFSET 都不发。
    ///
    /// 负控腿是「LAST_OFFSET 要发 GET_MAX_OFFSET」：没有它，把整段分支删掉（改成起点恒 0
    /// 且不查任何 offset）也能让主断言变绿。
    /// </summary>
    [Fact]
    public void LitePullFirstOffsetStartsAtZeroWithoutAMinOffsetRpc()
    {
        using var ep = OffsetEndpoint.Start();

        var first = new DefaultLitePullConsumer("LitePG_InitFirst");
        first.SetNamesrvAddr(ep.Addr);
        first.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        first.Assign(new[] { Queue0() });
        first.Start();
        try
        {
            Assert.True(WaitFor(() => ep.Requests(RequestCode.PullMessage).Count > 0, TimeSpan.FromSeconds(5)),
                "FIRST_OFFSET 的 lite-pull 必须真的发出第一笔拉取");
            List<WireReq> pulls = ep.Requests(RequestCode.PullMessage);
            Assert.Equal("0", pulls[0].Ext["queueOffset"]);
            Assert.Empty(ep.Requests(RequestCode.GetMinOffset));
        }
        finally
        {
            first.Shutdown();
        }

        // 负控：默认的 LAST_OFFSET 走 maxOffset —— 证明这份请求日志不是哑的
        var last = new DefaultLitePullConsumer("LitePG_InitLast");
        last.SetNamesrvAddr(ep.Addr);
        last.Assign(new[] { Queue0() });
        last.Start();
        try
        {
            Assert.True(WaitFor(() => ep.Requests(RequestCode.GetMaxOffset).Count > 0, TimeSpan.FromSeconds(5)),
                "LAST_OFFSET 的 lite-pull 必须发 GET_MAX_OFFSET");
        }
        finally
        {
            last.Shutdown();
        }

        Assert.Empty(ep.Requests(RequestCode.GetMinOffset));
    }

    /// <summary>
    /// 推送消费者同题（订阅模式走的是 Consumer.cs 的 ResolveInitialOffset）：新组没有已提交
    /// 位点时，起点 0 直接上线，不发 GET_MIN_OFFSET。
    /// </summary>
    [Fact]
    public void PushConsumerFirstOffsetStartsAtZeroWithoutAMinOffsetRpc()
    {
        using var ep = OffsetEndpoint.Start();

        var first = new DefaultMQPushConsumer("CG_InitFirst")
        {
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        first.SetNamesrvAddr(ep.Addr);
        first.Subscribe(Topic, "*");
        first.SetMessageListener(new NullListener());
        first.Start();
        try
        {
            Assert.True(WaitFor(() => ep.Requests(RequestCode.PullMessage).Count > 0, TimeSpan.FromSeconds(10)),
                "FIRST_OFFSET 的 push 消费者必须真的发出第一笔拉取；已到达的请求码："
                + string.Join(",", ep.Codes()));
            List<WireReq> pulls = ep.Requests(RequestCode.PullMessage);
            Assert.Equal("0", pulls[0].Ext["queueOffset"]);
            Assert.Empty(ep.Requests(RequestCode.GetMinOffset));
        }
        finally
        {
            first.Shutdown();
        }

        // 负控：LAST_OFFSET 那一支该发 maxOffset
        var last = new DefaultMQPushConsumer("CG_InitLast");
        last.SetNamesrvAddr(ep.Addr);
        last.Subscribe(Topic, "*");
        last.SetMessageListener(new NullListener());
        last.Start();
        try
        {
            Assert.True(WaitFor(() => ep.Requests(RequestCode.GetMaxOffset).Count > 0, TimeSpan.FromSeconds(10)),
                "LAST_OFFSET 的 push 消费者必须发 GET_MAX_OFFSET");
        }
        finally
        {
            last.Shutdown();
        }

        Assert.Empty(ep.Requests(RequestCode.GetMinOffset));
    }
}
