// 推送消费者 suspend()/resume()（挂起/恢复）的离线单测 —— 进程内假集群，不需要真集群。
//
// 对齐基准（Java 5.5.1，逐行读过）：
//   * DefaultMQPushConsumer#suspend():890 / #resume():898 / #isPause():902
//     → DefaultMQPushConsumerImpl#suspend():1312-1315 只置 pause=true 并记一条 info；
//       #resume():741-745 清标志 + **立刻 doRebalance()** 一次 + info。
//   * 标志的读点在 pullMessage:263-266 与 popMessage:518-521，退避都是
//     PULL_TIME_DELAY_MILLS_WHEN_SUSPEND=1000ms（:113）。
//   * 关键顺序：盖章 lastPullTimestamp 在 pullMessage:253，**位于挂起判定之前**。
//     写反了的后果很安静：挂起超过 120s（PULL_MAX_IDLE_TIME）后 rebalance 的停摆判据
//     （ProcessQueue#isPullExpired）把这条循环当死循环拆掉并打 "[BUG]doRebalance ...
//     because pull is pause" —— 一个只是被暂停的消费者被当成故障消费者处理。
//
// 为什么必须从 socket 上取证：suspend() 不抛错、不返回任何东西，唯一的可观测事实就是
// 「PULL_MESSAGE(11) 到底还上不上线」。这里挂起 2.5s 数请求，恢复后再数一次。
// 与 go/client/consumer_test.go 的 TestConsumerSuspendStopsPullsAndResumeRestartsThem、
// python/tests/test_consumer_suspend.py 同题。
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

public class SuspendResumeTests
{
    private const string Topic = "SuspendResumeTopic";
    private const string Broker = "broker-a";
    private const int MaxFrame = 20 * 1024 * 1024;

    /// <summary>一笔到达的请求：码 + 上线原样的 extFields。</summary>
    private sealed record WireReq(int Code, PropertyMap Ext);

    /// <summary>
    /// 假 namesrv + 假 broker：回路由、从心跳里认下 clientID 以便重平衡真能分到队列、
    /// 回「没有已提交位点」、回「这轮没有新消息」，并把每笔请求记账。
    /// </summary>
    private sealed class SuspendEndpoint : IDisposable
    {
        private readonly object _gate = new();
        private readonly List<WireReq> _reqs = new();
        private readonly Socket _listener;
        private string _clientId = string.Empty;

        public string Addr { get; }

        private SuspendEndpoint(Socket listener)
        {
            _listener = listener;
            Addr = ((IPEndPoint)listener.LocalEndPoint!).ToString();
            new Thread(AcceptLoop) { IsBackground = true }.Start();
        }

        public static SuspendEndpoint Start()
        {
            var server = new Socket(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
            server.Bind(new IPEndPoint(IPAddress.Loopback, 0));
            server.Listen(16);
            return new SuspendEndpoint(server);
        }

        public int Count(int code)
        {
            lock (_gate)
            {
                return _reqs.Count(r => r.Code == code);
            }
        }

        /// <summary>拉取请求里出现过的队列（消费组|queueId），用来验证挂起没有拆掉分配。</summary>
        public SortedSet<string> PulledQueues()
        {
            lock (_gate)
            {
                return new SortedSet<string>(_reqs
                    .Where(r => r.Code == RequestCode.PullMessage)
                    .Select(r => QueueKey(r.Ext)));
            }
        }

        private static string QueueKey(PropertyMap ext)
        {
            string group = ext.TryGetValue("consumerGroup", out string? g) ? g : "";
            string queueId = ext.TryGetValue("queueId", out string? q) ? q : "";
            return group + "|" + queueId;
        }

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

            if (req.Code == RequestCode.HeartBeat && req.Body.Length > 0
                && HeartbeatData.Decode(req.Body, out HeartbeatData hb) && hb.ClientId.Length > 0)
            {
                lock (_gate)
                {
                    _clientId = hb.ClientId;
                }
            }

            if (req.Code == RequestCode.GetRouteinfoByTopic)
            {
                RemotingCommand routeResp = Echo(req, ResponseCode.Success, null);
                routeResp.Body = BuildRoute(Addr);
                routeResp.HasBody = true;
                return routeResp;
            }

            if (req.Code == RequestCode.GetConsumerListByGroup && !req.IsOnewayRpc())
            {
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
                return Echo(req, ResponseCode.QueryNotFound, "mock: no committed offset");
            }

            if (req.Code is RequestCode.GetMaxOffset or RequestCode.GetMinOffset && !req.IsOnewayRpc())
            {
                RemotingCommand offsetResp = Echo(req, ResponseCode.Success, null);
                offsetResp.AddExtField("offset", "0");
                return offsetResp;
            }

            if (req.Code == RequestCode.PullMessage && !req.IsOnewayRpc())
            {
                RemotingCommand pullResp = Echo(req, ResponseCode.PullNotFound, null);
                pullResp.AddExtField("nextBeginOffset", "0");
                pullResp.AddExtField("minOffset", "0");
                pullResp.AddExtField("maxOffset", "0");
                return pullResp;
            }

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

    [Fact]
    public void SuspendStopsPullsAndResumeRestartsThem()
    {
        using SuspendEndpoint endpoint = SuspendEndpoint.Start();

        var consumer = new DefaultMQPushConsumer("CG_parity_suspend")
        {
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        consumer.SetNamesrvAddr(endpoint.Addr);
        consumer.Subscribe(Topic, "*");
        consumer.SetMessageListener(new NullListener());
        consumer.Start();
        try
        {
            string key = DefaultMQPushConsumer.OffsetKeyForTest(new MessageQueue(Topic, Broker, 0));

            Assert.True(WaitFor(() => endpoint.Count(RequestCode.PullMessage) >= 2, TimeSpan.FromSeconds(10)),
                "挂起之前拉取循环必须真的在打；已到达的请求码：" + string.Join(",", endpoint.Codes()));

            consumer.Suspend();
            Assert.True(consumer.IsPaused, "isPause() 要如实反映挂起（Java :902）");

            // 挂起前已在途的那一笔先落地，再取基线，否则把合法的一笔算成漏网；
            // 300ms 也给了循环一次「盖章 + 停在挂起闸门」的时间。
            Thread.Sleep(600);
            int baseline = endpoint.Count(RequestCode.PullMessage);
            long stampBefore = consumer.LastPullAt(key);
            Thread.Sleep(2500);

            int pausedCount = endpoint.Count(RequestCode.PullMessage);
            Assert.True(pausedCount == baseline,
                "挂起期间不能有新的 PULL_MESSAGE 上线（Java pullMessage:263-266）："
                + $"基线 {baseline}，挂起 2.5s 后 {pausedCount}");

            // 顺序不变量：盖章在挂起判定**之前**，所以暂停的循环仍在按时盖章，
            // 120s 停摆判据不会把一个只是挂起的消费者判成死循环。
            Assert.True(consumer.LastPullAt(key) > stampBefore,
                "挂起期间 lastPullTimestamp 必须继续推进，否则 isPullExpired 会误拆这条循环");
            Assert.False(consumer.PullStalledForTest(key, UtilAll.CurrentTimeMillis()),
                "挂起不等于停摆：自愈判据在这段窗口里必须保持安静");

            SortedSet<string> queuesBefore = endpoint.PulledQueues();
            consumer.Resume();
            Assert.False(consumer.IsPaused);

            Assert.True(WaitFor(() => endpoint.Count(RequestCode.PullMessage) > baseline, TimeSpan.FromSeconds(5)),
                "Resume() 之后拉取必须恢复");
            Assert.True(queuesBefore.SetEquals(endpoint.PulledQueues()),
                "挂起/恢复只是暂停发请求，分配集必须原地保留（Java 不动 ProcessQueueTable）");
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    /// <summary>
    /// 幂等腿：Java 的 suspend()/resume() 都只是置标志、不判前置状态，重复调用不抛错，
    /// 未 Start() 的消费者同样可以置标志（挂起语义由拉取循环读标志时生效）。
    /// </summary>
    [Fact]
    public void SuspendAndResumeAreIdempotentAndSafeBeforeStart()
    {
        var consumer = new DefaultMQPushConsumer("CG_parity_suspend_api");
        Assert.False(consumer.IsPaused);

        consumer.Suspend();
        consumer.Suspend();
        Assert.True(consumer.IsPaused);

        consumer.Resume();
        consumer.Resume();
        Assert.False(consumer.IsPaused);
    }
}
