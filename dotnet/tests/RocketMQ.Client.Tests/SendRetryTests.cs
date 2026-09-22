// 发送重试内核（Java DefaultMQProducerImpl#sendDefaultImpl）的离线对拍。
//
// 真集群造不出 SYSTEM_BUSY，也造不出「慢 broker 把总预算吃光」，而这两条恰好是这段
// 内核的全部难点，所以这里在进程内起一个**假集群**（1 个 namesrv + N 个 broker，只说
// remoting 协议），把每个 broker 的应答码和应答延迟脚本化。
//
// 与 python/tests/test_send_retry.py、cpp/tests/test_send_retry.cpp、
// rust/src/client/producer/send_retry_tests.rs 同题。
using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;
// PropertyMap 是 src 侧的 global using 别名（SortedDictionary<string,string>），测试项目要显式声明。
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

public class SendRetryTests
{
    private const string Topic = "T1";
    private const int MaxFrame = 20 * 1024 * 1024;

    // ---------------------------------------------------------------- 假集群

    /// <summary>
    /// 一笔上线报文的取证：请求码（发送/路由/心跳…）、extFields 和原始 body。
    /// 客户端自说自话不算证据，只有从 socket 上抓下来的字段才能证明钩子真的生效了。
    /// </summary>
    private sealed class WireRecord
    {
        public int Code { get; init; }
        public PropertyMap Ext { get; init; } = new();
        public byte[] Body { get; init; } = Array.Empty<byte>();
        public bool HasBody { get; init; }

        public static WireRecord Of(RemotingCommand req) => new()
        {
            Code = req.Code,
            Ext = new PropertyMap(req.ExtFields),
            Body = req.Body,
            HasBody = req.HasBody,
        };

        /// <summary>还原成裸报文，用来做 broker 侧的验签复算。</summary>
        public RemotingCommand ToCommand()
        {
            var cmd = RemotingCommand.CreateRequestCommand(Code, null);
            foreach ((string key, string value) in Ext)
            {
                cmd.AddExtField(key, value);
            }

            cmd.Body = Body;
            cmd.HasBody = HasBody;
            return cmd;
        }
    }

    /// <summary>取证的条数上限：后台线程会持续打，留几百条足够判断"有没有打标"。</summary>
    private const int RequestLogCap = 500;

    /// <summary>单个 broker 的脚本：按顺序弹出 (应答码, 应答前 sleep 毫秒)，耗尽后一直用 Tail。</summary>
    private sealed class BrokerScript
    {
        public Queue<(int Code, int DelayMs)> Steps { get; } = new();
        public (int Code, int DelayMs) Tail { get; set; } = (ResponseCode.Success, 0);
        public int Requests;
    }

    /// <summary>
    /// 进程内假集群。broker 名固定为 broker-0..N，路由里的队列顺序与之一致，
    /// 因此第一次发送必然落在 broker-0（选队是轮询，游标从 0 起）。
    /// </summary>
    private sealed class MockCluster : IDisposable
    {
        private readonly object _gate = new();
        private readonly List<BrokerScript> _brokers = new();
        private readonly List<string> _brokerAddrs = new();
        private readonly List<Socket> _sockets = new();
        private readonly List<WireRecord> _requests = new();
        private bool _routeOk = true;

        public string NamesrvAddr { get; private init; } = string.Empty;

        /// <summary>起 brokerCount 个真监听的 broker + 一个 namesrv。</summary>
        public static MockCluster Start(int brokerCount, bool routeOk = true)
        {
            var listeners = new List<Socket>();
            var addrs = new List<string>();
            for (int i = 0; i < brokerCount; ++i)
            {
                Socket l = BindLoopback();
                listeners.Add(l);
                addrs.Add(EndPointOf(l));
            }

            MockCluster cluster = WithAddrs(addrs, routeOk);
            for (int i = 0; i < listeners.Count; ++i)
            {
                int index = i;
                Serve(listeners[i], req => cluster.BrokerRespond(index, req));
            }

            return cluster;
        }

        /// <summary>只起 namesrv，路由指向给定地址（可以用一个已经关掉的端口造连接失败）。</summary>
        public static MockCluster WithAddrs(List<string> brokerAddrs, bool routeOk = true)
        {
            Socket namesrv = BindLoopback();
            var cluster = new MockCluster
            {
                NamesrvAddr = EndPointOf(namesrv),
                _routeOk = routeOk,
            };
            lock (cluster._gate)
            {
                cluster._brokerAddrs.AddRange(brokerAddrs);
                for (int i = 0; i < brokerAddrs.Count; ++i)
                {
                    cluster._brokers.Add(new BrokerScript());
                }
            }

            Serve(namesrv, cluster.NamesrvRespond);
            return cluster;
        }

        /// <summary>脚本化第 index 个 broker：先按 steps 依次应答，之后一直用 tail。</summary>
        public void Script(int index, List<(int Code, int DelayMs)> steps,
            (int Code, int DelayMs) tail)
        {
            lock (_gate)
            {
                BrokerScript broker = _brokers[index];
                broker.Steps.Clear();
                foreach ((int, int) step in steps)
                {
                    broker.Steps.Enqueue(step);
                }

                broker.Tail = tail;
                broker.Requests = 0;
            }
        }

        /// <summary>第 index 个 broker 收到的 SEND 请求数。</summary>
        public int Requests(int index)
        {
            lock (_gate)
            {
                return _brokers[index].Requests;
            }
        }

        /// <summary>记下每一笔上线报文（namesrv 与 broker 都算），供钩子类断言回查。</summary>
        private void Record(RemotingCommand req)
        {
            lock (_gate)
            {
                if (_requests.Count < RequestLogCap)
                {
                    _requests.Add(WireRecord.Of(req));
                }
            }
        }

        public void ClearRequests()
        {
            lock (_gate)
            {
                _requests.Clear();
            }
        }

        /// <summary>指定请求码上收到过多少笔请求。</summary>
        public int CountRequests(int code)
        {
            lock (_gate)
            {
                return _requests.Count(r => r.Code == code);
            }
        }

        /// <summary>指定请求码上，extFields 里 key=value 命中了多少笔。</summary>
        public int CountRequestsWith(int code, string key, string value)
        {
            lock (_gate)
            {
                return _requests.Count(r =>
                    r.Code == code && r.Ext.TryGetValue(key, out string? v) && v == value);
            }
        }

        /// <summary>
        /// 第一笔**发送**请求的取证：本端口按 Java 的三级判据发 SendMessageV2(310) /
        /// SendBatchMessage(320) / SendReplyMessageV2(325)，写死某一个码会永远抓不到。
        /// </summary>
        public WireRecord? FirstSendRequest()
        {
            lock (_gate)
            {
                return _requests.FirstOrDefault(r => IsSendCode(r.Code));
            }
        }

        /// <summary>指定请求码里是否有任何一笔带了这个 extField。</summary>
        public bool AnyRequestHas(int code, string key)
        {
            lock (_gate)
            {
                return _requests.Any(r => r.Code == code && r.Ext.ContainsKey(key));
            }
        }

        public void Dispose()
        {
            foreach (Socket s in _sockets)
            {
                try
                {
                    s.Dispose();
                }
                catch (Exception)
                {
                    // 关闭监听端口时不需要处理任何异常
                }
            }
        }

        private RemotingCommand? NamesrvRespond(RemotingCommand req)
        {
            Record(req);
            if (req.Code == RequestCode.GetRouteinfoByTopic)
            {
                List<string> addrs;
                bool ok;
                lock (_gate)
                {
                    addrs = new List<string>(_brokerAddrs);
                    ok = _routeOk;
                }

                if (!ok)
                {
                    return Respond(req, ResponseCode.TopicNotExist, "mock: no route");
                }

                RemotingCommand resp = Respond(req, ResponseCode.Success, null);
                resp.Body = BuildRoute(addrs);
                resp.HasBody = true;
                return resp;
            }

            return Respond(req, ResponseCode.Success, null);
        }

        private RemotingCommand? BrokerRespond(int index, RemotingCommand req)
        {
            Record(req);
            if (!IsSendCode(req.Code))
            {
                // 心跳等非发送请求一律应答成功，别让后台线程卡在错误上
                return req.IsOnewayRpc() ? null : Respond(req, ResponseCode.Success, null);
            }

            int code;
            int delay;
            int seq;
            lock (_gate)
            {
                BrokerScript broker = _brokers[index];
                (int, int) step = broker.Steps.Count > 0 ? broker.Steps.Dequeue() : broker.Tail;
                (code, delay) = step;
                broker.Requests++;
                seq = broker.Requests;
            }

            if (delay > 0)
            {
                Thread.Sleep(delay);
            }

            if (req.IsOnewayRpc())
            {
                return null;
            }

            RemotingCommand resp = Respond(req, code,
                code == ResponseCode.Success ? null : "mock failure");
            if (IsStoreStatus(code))
            {
                // 只有 broker 真的收了消息才有 msgId/queueOffset 可回
                resp.AddExtField("msgId", "MOCK-" + index.ToString(CultureInfo.InvariantCulture)
                                             + "-" + seq.ToString(CultureInfo.InvariantCulture));
                resp.AddExtField("queueId", "0");
                resp.AddExtField("queueOffset", seq.ToString(CultureInfo.InvariantCulture));
            }

            return resp;
        }

        private static bool IsStoreStatus(int code) =>
            code is ResponseCode.Success or ResponseCode.FlushDiskTimeout
                or ResponseCode.SlaveNotAvailable or ResponseCode.FlushSlaveTimeout;

        private static bool IsSendCode(int code) =>
            code is RequestCode.SendMessage or RequestCode.SendMessageV2
                or RequestCode.SendBatchMessage or RequestCode.SendReplyMessage
                or RequestCode.SendReplyMessageV2;

        /// <summary>路由里的队列顺序与 broker 名一致，且每台只有一个队列。</summary>
        private static byte[] BuildRoute(List<string> brokerAddrs)
        {
            var route = new TopicRouteData();
            for (int i = 0; i < brokerAddrs.Count; ++i)
            {
                string name = BrokerName(i);
                route.QueueDatas.Add(new QueueData(name, 1, 1,
                    PermName.PermRead | PermName.PermWrite, 0));
                route.BrokerDatas.Add(new BrokerData("MockCluster", name,
                    new SortedDictionary<long, string> { { MixAll.MasterId, brokerAddrs[i] } }));
            }

            return route.Encode();
        }

        private static string BrokerName(int index) =>
            "broker-" + index.ToString(CultureInfo.InvariantCulture);

        private static RemotingCommand Respond(RemotingCommand req, int code, string? remark)
        {
            var resp = RemotingCommand.CreateResponseCommand(code, remark);
            // 客户端按 opaque 配对，串了的响应会被当噪声丢掉
            resp.Opaque = req.Opaque;
            resp.SerializeTypeCurrentRpc = req.SerializeTypeCurrentRpc;
            return resp;
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

        /// <summary>
        /// accept 循环：每条连接一个线程。这里刻意用**专用线程**而不是 Task ——
        /// 读帧和脚本化延迟都是阻塞调用，几百个卡在 Read 上的工作项会让线程池饥饿，
        /// 实测把下一次 accept 拖到秒级，计时断言就成了假失败。
        /// </summary>
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

                RemotingCommand? resp = respond(req);
                if (resp is null)
                {
                    continue;
                }

                byte[] wire = resp.Encode();
                stream.Write(wire, 0, wire.Length);
            }
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

    // ---------------------------------------------------------------- 用例

    private static DefaultMQProducer Started(MockCluster cluster, string group)
    {
        var producer = new DefaultMQProducer(group)
        {
            NamesrvAddr = cluster.NamesrvAddr,
            InstanceName = group,
        };
        producer.Start();
        return producer;
    }

    private static Message Msg() => new(Topic, Encoding.UTF8.GetBytes("hello"));

    /// <summary>默认集合与 Java DefaultMQProducer#retryResponseCodes 逐项一致（8 个码）。</summary>
    [Fact]
    public void DefaultRetryResponseCodes_MatchJava()
    {
        var producer = new DefaultMQProducer("GID_RetryCodes");
        Assert.Equal(8, producer.RetryResponseCodes.Count);
        foreach (int code in new[]
                 {
                     ResponseCode.SystemError, ResponseCode.SystemBusy,
                     ResponseCode.ServiceNotAvailable, ResponseCode.NoPermission,
                     ResponseCode.TopicNotExist, ResponseCode.NoBuyerId,
                     ResponseCode.NotInCurrentUnit, ResponseCode.GoAway,
                 })
        {
            Assert.True(producer.IsRetryResponseCode(code), "code=" + code);
        }

        // 没等到响应码（连接就没建立）等于不可重试；不在集合里的码也不可重试
        Assert.False(producer.IsRetryResponseCode(null));
        Assert.False(producer.IsRetryResponseCode(ResponseCode.MessageIllegal));

        producer.AddRetryResponseCode(ResponseCode.MessageIllegal);
        Assert.True(producer.IsRetryResponseCode(ResponseCode.MessageIllegal));
        // RetryResponseCodes 给的是副本，改它不影响判定
        producer.RetryResponseCodes.Clear();
        Assert.True(producer.IsRetryResponseCode(ResponseCode.MessageIllegal));
    }

    /// <summary>可重试的 broker 响应码：换一台重发，最终落在那台健康的 broker 上。</summary>
    [Fact]
    public void RetryableBrokerCode_SwitchesToAnotherBroker()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.ServiceNotAvailable, 0));
        cluster.Script(1, new List<(int, int)>(), (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_RetryOk");

        SendResult result = producer.Send(Msg());
        Assert.Equal(SendStatus.SendOk, result.SendStatus);
        Assert.Equal(1, cluster.Requests(0));
        Assert.Equal(1, cluster.Requests(1));
        producer.Shutdown();
    }

    /// <summary>不可重试的响应码：一次都不该重试，异常原样抛给调用方。</summary>
    [Fact]
    public void NonRetryableBrokerCode_ThrowsAtOnce()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.MessageIllegal, 0));
        DefaultMQProducer producer = Started(cluster, "GID_RetryNoRetry");

        MQBrokerException e = Assert.Throws<MQBrokerException>(() => producer.Send(Msg()));
        Assert.Equal(ResponseCode.MessageIllegal, e.ResponseCode);
        Assert.Equal(1, cluster.Requests(0));
        Assert.Equal(0, cluster.Requests(1));
        producer.Shutdown();
    }

    /// <summary>加了RetryResponseCode 之后，原本不可重试的码也要换 broker。</summary>
    [Fact]
    public void AddedRetryResponseCode_MakesCodeRetryable()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.MessageIllegal, 0));
        DefaultMQProducer producer = Started(cluster, "GID_RetryAdded");
        producer.AddRetryResponseCode(ResponseCode.MessageIllegal);

        SendResult result = producer.Send(Msg());
        Assert.Equal(SendStatus.SendOk, result.SendStatus);
        Assert.Equal(1, cluster.Requests(0));
        Assert.Equal(1, cluster.Requests(1));
        producer.Shutdown();
    }

    /// <summary>重试耗尽：错误码取最后一次失败的 broker 响应码，文案带上打过交道的 broker 列表。</summary>
    [Fact]
    public void ExhaustedRetries_ReportBrokersSentAndCode()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.ServiceNotAvailable, 0));
        DefaultMQProducer producer = Started(cluster, "GID_RetryExhaust");
        producer.RetryTimesWhenSendFailed = 2;

        MQClientException e = Assert.Throws<MQClientException>(() => producer.Send(Msg()));
        Assert.Equal(ResponseCode.ServiceNotAvailable, e.ResponseCode);
        Assert.Equal(3, cluster.Requests(0));
        Assert.Contains("Send [3] times, still failed", e.Message);
        Assert.Contains("Topic: " + Topic, e.Message);
        Assert.Contains("BrokersSent: [broker-0, broker-0, broker-0]", e.Message);
        producer.Shutdown();
    }

    /// <summary>路由完全取不到：循环外就按 NOT_FOUND_TOPIC 定性，不把重试次数空转掉。</summary>
    [Fact]
    public void MissingRoute_FailsFastWithNotFoundTopicCode()
    {
        using var cluster = MockCluster.Start(1, routeOk: false);
        DefaultMQProducer producer = Started(cluster, "GID_RetryNoRoute");

        MQClientException e = Assert.Throws<MQClientException>(() => producer.Send(Msg()));
        Assert.Equal(ClientErrorCode.NotFoundTopicException, e.ResponseCode);
        // 一次 broker 都没联系过 —— 失败发生在选队列之前
        Assert.Equal(0, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>连不上 broker：定性成 CONNECT_BROKER_EXCEPTION(10001)，而不是原样冒泡。</summary>
    [Fact]
    public void ConnectFailure_IsQualifiedWith10001()
    {
        // 借一个刚释放的本地端口当"死 broker"：路由有效，但三次尝试都连不上
        Socket probe = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        string deadAddr = ((IPEndPoint)probe.LocalEndPoint!).ToString();
        probe.Dispose();

        using var cluster = MockCluster.WithAddrs(new List<string> { deadAddr });
        DefaultMQProducer producer = Started(cluster, "GID_RetryConnect");
        producer.RetryTimesWhenSendFailed = 2;

        MQClientException e = Assert.Throws<MQClientException>(() => producer.Send(Msg()));
        Assert.Equal(ClientErrorCode.ConnectBrokerException, e.ResponseCode);
        Assert.Contains("Send [3] times, still failed", e.Message);
        producer.Shutdown();
    }

    /// <summary>单次超时上限：慢 broker 不能把整个预算吃光，剩下的 broker 还有机会。</summary>
    [Fact]
    public void PerRequestTimeout_CapsSlowBrokerAndRetries()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)> { (ResponseCode.Success, 700) },
            (ResponseCode.Success, 0));
        DefaultMQProducer producer = Started(cluster, "GID_RetryPerReq");
        producer.SendMsgMaxTimeoutPerRequest = 150;
        producer.SendMsgTimeout = 3000;

        var watch = Stopwatch.StartNew();
        SendResult result = producer.Send(Msg());
        watch.Stop();

        Assert.Equal(SendStatus.SendOk, result.SendStatus);
        // 没有上限的话第一次就要等满 700ms 才返回；这里 150ms 就放弃了那台慢 broker
        Assert.True(watch.ElapsedMilliseconds < 500,
            "elapsed=" + watch.ElapsedMilliseconds.ToString(CultureInfo.InvariantCulture));
        Assert.Equal(1, cluster.Requests(1));
        producer.Shutdown();
    }

    /// <summary>
    /// 总预算用完：抛 RemotingTooMuchRequestException 而不是「重试耗尽」，且第二笔不许上线。
    ///
    /// ⚠ 预算是让**发送钩子睡 120ms** 吃掉的，不靠 broker 的应答延迟：后者的耗时恰好等于
    /// 本次调用的预算（100ms 的等待在 100.0~100.9ms 之间返回完全正常），而判定是
    /// <c>timeout &lt; costTime</c> 的整数毫秒比较（Java 用 currentTimeMillis，同样如此），
    /// 卡在边界上就会偶发地多跑一笔 0ms 预算的尝试、最后抛成「重试耗尽」——实测整跑测试集时
    /// 会随机红一次。python 同题用例（test_send_retry.py
    /// ::test_call_timeout_stops_retrying_and_raises_too_much_request）用的是同一招：
    /// 往发送路径注入 first_send_sleep_ms=60 &gt; timeout=30。
    /// </summary>
    [Fact]
    public void ExhaustedBudget_ReportsCallTimeout()
    {
        using var cluster = MockCluster.Start(1);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.SystemError, 0));
        var producer = new DefaultMQProducer("GID_RetryBudget")
        {
            NamesrvAddr = cluster.NamesrvAddr,
            InstanceName = "GID_RetryBudget",
            SendMsgTimeout = 100,
            RetryTimesWhenSendFailed = 2,
        };
        producer.RegisterSendMessageHook(new SleepBeforeHook(120));
        producer.Start();

        Assert.Throws<RemotingTooMuchRequestException>(() => producer.Send(Msg()));
        Assert.Equal(1, cluster.Requests(0));
        producer.Shutdown();
    }

    /// <summary>在 SendMessageBefore 里睡 fixed 毫秒：把耗时确定地加进发送路径本身。</summary>
    private sealed class SleepBeforeHook : ISendMessageHook
    {
        private readonly int _millis;

        public SleepBeforeHook(int millis) => _millis = millis;

        public string HookName() => "sleep-before";

        public void SendMessageBefore(SendMessageContext context) => Thread.Sleep(_millis);

        public void SendMessageAfter(SendMessageContext context)
        {
        }
    }

    /// <summary>「存了但没存好」：默认原样返回，开了开关才换 broker。</summary>
    [Fact]
    public void NotStoreOk_OnlySwitchesBrokerWhenConfigured()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.FlushDiskTimeout, 0));

        DefaultMQProducer off = Started(cluster, "GID_NotStoreOff");
        SendResult kept = off.Send(Msg());
        Assert.Equal(SendStatus.FlushDiskTimeout, kept.SendStatus);
        Assert.Equal(1, cluster.Requests(0));
        Assert.Equal(0, cluster.Requests(1));
        off.Shutdown();

        cluster.Script(0, new List<(int, int)>(), (ResponseCode.FlushDiskTimeout, 0));
        DefaultMQProducer on = Started(cluster, "GID_NotStoreOn");
        on.RetryAnotherBrokerWhenNotStoreOk = true;
        SendResult switched = on.Send(Msg());
        Assert.Equal(SendStatus.SendOk, switched.SendStatus);
        Assert.Equal(1, cluster.Requests(0));
        Assert.Equal(1, cluster.Requests(1));
        on.Shutdown();
    }

    /// <summary>故障规避打开时：失败的 broker 要被隔离（不可选），成功的那台要记下真实延迟。</summary>
    [Fact]
    public void FailedBroker_IsIsolated_AndLatencyIsRecorded()
    {
        using var cluster = MockCluster.Start(2);
        cluster.Script(0, new List<(int, int)> { (ResponseCode.ServiceNotAvailable, 30) },
            (ResponseCode.ServiceNotAvailable, 30));
        DefaultMQProducer producer = Started(cluster, "GID_RetryFault");
        producer.SendLatencyFaultEnable = true;

        SendResult result = producer.Send(Msg());
        Assert.Equal(SendStatus.SendOk, result.SendStatus);

        LatencyFaultToleranceImpl tolerance = producer.MqFaultStrategy.LatencyFaultTolerance;
        Assert.False(tolerance.IsAvailable("broker-0"), "回错误的 broker 必须被隔离");
        FaultItem? failed = tolerance.GetFaultItem("broker-0");
        Assert.NotNull(failed);
        // 记的仍是真实延迟（~30ms），而不是隔离档位（10000ms）
        Assert.InRange(failed!.CurrentLatency, 10.0, 600.0);
        Assert.False(failed.IsReachable());
        FaultItem? ok = tolerance.GetFaultItem("broker-1");
        Assert.NotNull(ok);
        Assert.True(ok!.IsAvailable());
        Assert.True(ok.IsReachable());
        producer.Shutdown();
    }

    // ------------------------------------------------------- 请求钩子真的写到 socket 上

    private const string WireSk = "SK_wire_12345678";

    /// <summary>
    /// 钩子的四段取证，全部只看**抓下来的报文**：
    /// ① 只注册 ACL → 有 AccessKey/Signature、没有 ReqT；
    /// ② ACL + stream → 有 ReqT="0"，且把抓到的报文按 broker 的口径复算 HMAC 能对上
    ///    （证明 ReqT 落在签名内容里，而不是签完之后又改了几个字段）；
    /// ③ lite 消费者（Java 默认开 stream）→ 路由与心跳都带 ReqT；
    /// ④ 关掉开关 → 一笔都不带，但请求照发（不是"没打出去"造成的假绿）。
    /// 顺序在 RequestHooks.Compose 里，绑定位置在各 facade 的 Start() 里，两处都得线上验证。
    /// </summary>
    [Fact]
    public void RequestHooksReachTheWire()
    {
        // ---- ① 只有 ACL：签名字段在，ReqT 不在 ----
        using (var cluster = MockCluster.Start(1))
        {
            DefaultMQProducer producer = StartedWithHook(cluster, "GID_HookAclOnly", false);
            Assert.Equal(SendStatus.SendOk, producer.Send(Msg()).SendStatus);

            WireRecord? rec = cluster.FirstSendRequest();
            Assert.NotNull(rec);
            Assert.Equal("AK_wire", rec!.Ext[SessionCredentials.AccessKeyField]);
            Assert.True(rec.Ext.ContainsKey(SessionCredentials.SignatureField));
            Assert.False(rec.Ext.ContainsKey(MixAll.ReqT), "没开 stream 不该打 ReqT");
            producer.Shutdown();
        }

        // ---- ② ACL + stream：ReqT 在签**之前**写入，所以算进签名 ----
        using (var cluster = MockCluster.Start(1))
        {
            DefaultMQProducer producer = StartedWithHook(cluster, "GID_HookStreamAcl", true);
            Assert.Equal(SendStatus.SendOk, producer.Send(Msg()).SendStatus);

            WireRecord rec = cluster.FirstSendRequest()!;
            Assert.Equal("0", rec.Ext[MixAll.ReqT]); // Java 写 code 的字符串形式，不是枚举名
            // broker 侧复算：拿抓到的报文（含 ReqT、排除 Signature）重算一遍签名
            Assert.Equal(rec.Ext[SessionCredentials.SignatureField],
                AclClientRPCHook.CalcSignature(WireSk, rec.ToCommand()));
            producer.Shutdown();
        }

        // ---- ③ lite 消费者默认开 stream：路由 + 心跳都带标 ----
        using (var cluster = MockCluster.Start(1))
        {
            DefaultLitePullConsumer consumer = StartedLite(cluster, "GID_HookLite", stream: null);
            Assert.True(cluster.CountRequestsWith(RequestCode.GetRouteinfoByTopic,
                MixAll.ReqT, "0") > 0, "路由请求要带 ReqT");
            Assert.Equal(cluster.CountRequests(RequestCode.GetRouteinfoByTopic),
                cluster.CountRequestsWith(RequestCode.GetRouteinfoByTopic, MixAll.ReqT, "0"));
            Assert.True(cluster.CountRequestsWith(RequestCode.HeartBeat, MixAll.ReqT, "0") > 0,
                "心跳要带 ReqT");
            consumer.Shutdown();
        }

        // ---- ④ 显式关掉：一笔都不带，但请求确实发出去了 ----
        using (var cluster = MockCluster.Start(1))
        {
            DefaultLitePullConsumer consumer = StartedLite(cluster, "GID_HookLiteOff", stream: false);
            Assert.True(cluster.CountRequests(RequestCode.GetRouteinfoByTopic) > 0);
            Assert.False(cluster.AnyRequestHas(RequestCode.GetRouteinfoByTopic, MixAll.ReqT));
            Assert.False(cluster.AnyRequestHas(RequestCode.HeartBeat, MixAll.ReqT));
            consumer.Shutdown();
        }
    }

    /// <summary>
    /// 发送请求码的三级判据（Java MQClientAPIImpl#sendMessage:550-563）：先判 isReply
    /// ⇒ 325，再判「这条消息是不是批量」⇒ SendBatchMessage(320)，否则 310。
    ///
    /// ⚠ 请求码与 V2 头的单字母键 <c>m</c>（batch）是两件事：broker 按 <c>m</c> 选
    /// sendBatchMessage 还是单条写入（SendMessageProcessor:117 读 requestHeader.isBatch()），
    /// 码只影响服务端按码归类（proxy AbstractRemotingActivity:69 与 auth
    /// DefaultAuthorizationContextBuilder:230-240 都把 310/320 列在同一个 case 里）。
    /// 所以两个都取证：只对码不对 <c>m</c>，批量 body 会被按单条解析。
    /// </summary>
    [Fact]
    public void SendRequestCodeFollowsJava()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQProducer producer = Started(cluster, "GID_SendCodeBranch");

        cluster.ClearRequests();
        Assert.Equal(SendStatus.SendOk, producer.Send(Msg()).SendStatus);
        WireRecord single = cluster.FirstSendRequest()!;
        Assert.Equal(RequestCode.SendMessageV2, single.Code);
        Assert.Equal("false", single.Ext["m"]);

        cluster.ClearRequests();
        Assert.Equal(SendStatus.SendOk,
            producer.SendBatch(new List<Message> { Msg(), Msg() }).SendStatus);
        WireRecord batch = cluster.FirstSendRequest()!;
        Assert.Equal(RequestCode.SendBatchMessage, batch.Code);
        Assert.Equal("true", batch.Ext["m"]);

        // reply 判在 batch 之前：带 MSG_TYPE=reply 的批量仍然走 325。
        cluster.ClearRequests();
        Message replyBatch = Msg();
        replyBatch.IsBatch = true;
        replyBatch.Properties[MessageConst.PropertyMessageType] = MixAll.REPLY_MESSAGE_FLAG;
        Assert.Equal(SendStatus.SendOk, producer.Send(replyBatch).SendStatus);
        WireRecord reply = cluster.FirstSendRequest()!;
        Assert.Equal(RequestCode.SendReplyMessageV2, reply.Code);
        Assert.Equal("true", reply.Ext["m"]);

        producer.Shutdown();
    }

    /// <summary>
    /// 带 ACL 钩子的生产者。钩子必须排在 Start() **之前**：Java 的 rpcHook 随
    /// <c>MQClientAPIImpl</c> 构造传入，本端口各 facade 也在 <c>Start()</c> 里把它绑到
    /// 传输层 —— 启动后再 SetRpcHook 已经来不及，报文会裸着出去（这正是 ① ② 要抓的东西）。
    /// </summary>
    private static DefaultMQProducer StartedWithHook(MockCluster cluster, string group, bool stream)
    {
        var producer = new DefaultMQProducer(group)
        {
            NamesrvAddr = cluster.NamesrvAddr,
            InstanceName = group,
            EnableStreamRequestType = stream,
        };
        producer.SetRpcHook(new AclClientRPCHook(new SessionCredentials("AK_wire", WireSk)));
        producer.Start();
        return producer;
    }

    /// <summary>
    /// 起一个 lite 消费者：stream 传 null 表示**不动默认值**（Java 的
    /// DefaultLitePullConsumer 在构造函数里就置 true，③ 段要验的正是这个默认）。
    /// </summary>
    private static DefaultLitePullConsumer StartedLite(MockCluster cluster, string group,
        bool? stream)
    {
        var consumer = new DefaultLitePullConsumer(group);
        consumer.SetInstanceName(group);
        if (stream.HasValue)
        {
            consumer.EnableStreamRequestType = stream.Value;
        }

        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.Subscribe(Topic, "*");
        consumer.Start();
        return consumer;
    }
}
