// 进程内假集群（1 个 namesrv + N 个 broker，只说 remoting 协议）。
//
// 真集群造不出 SYSTEM_BUSY，也造不出「慢 broker 把总预算吃光」，而这两条恰好是发送内核
// 的全部难点，所以这里把每个 broker 的应答码、应答延迟与「干脆不回」脚本化。
//
// 从 SendRetryTests 抽出来说给两类用例共用：同步重试内核（SendRetryTests）与异步发送
// 内核（ProducerAsyncTests）。与 python/tests/test_send_retry.py、
// cpp/tests/test_producer_async.cpp、rust/src/client/producer/send_retry_tests.rs 同题。
using System.Buffers.Binary;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
// PropertyMap 是 src 侧的 global using 别名（SortedDictionary<string,string>），测试项目要显式声明。
using PropertyMap = System.Collections.Generic.SortedDictionary<string, string>;

namespace RocketMQ.Client.Tests;

/// <summary>
/// 一笔上线报文的取证：请求码（发送/路由/心跳…）、opaque、extFields 和原始 body。
/// 客户端自说自话不算证据，只有从 socket 上抓下来的字段才能证明钩子真的生效了。
/// </summary>
internal sealed class WireRecord
{
    public int Code { get; init; }

    /// <summary>上线时的 opaque（重试链要求每轮换新值，否则两次尝试的应答串台）。</summary>
    public int Opaque { get; init; }

    public PropertyMap Ext { get; init; } = new();
    public byte[] Body { get; init; } = Array.Empty<byte>();
    public bool HasBody { get; init; }

    public static WireRecord Of(RemotingCommand req) => new()
    {
        Code = req.Code,
        Opaque = req.Opaque,
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

/// <summary>
/// 进程内假集群。broker 名固定为 broker-0..N，路由里的队列顺序与之一致，
/// 因此第一次发送必然落在 broker-0（选队是轮询，游标从 0 起）。
/// </summary>
internal sealed class MockCluster : IDisposable
{
    /// <summary>脚本里表示「收下沉不应答」的哨兵应答码：把客户端逼到超时。</summary>
    public const int NoReply = -1;

    /// <summary>取证的条数上限：后台线程会持续打，留几百条足够判断"有没有打标"。</summary>
    private const int RequestLogCap = 500;

    /// <summary>单帧长度上限，超了直接断开（和客户端的判据一致）。</summary>
    private const int MaxFrame = 20 * 1024 * 1024;

    private readonly object _gate = new();
    private readonly List<BrokerScript> _brokers = new();
    private readonly List<string> _brokerAddrs = new();
    private readonly List<Socket> _sockets = new();
    private readonly List<WireRecord> _requests = new();
    private bool _routeOk = true;

    public string NamesrvAddr { get; private init; } = string.Empty;

    /// <summary>路由里 broker-N 的地址（与下标同序）；<see cref="DeadAddr"/> 那几台没人监听。</summary>
    public IReadOnlyList<string> BrokerAddrs
    {
        get
        {
            lock (_gate)
            {
                return new List<string>(_brokerAddrs);
            }
        }
    }

    /// <summary>单个 broker 的脚本：按顺序弹出 (应答码, 应答前 sleep 毫秒)，耗尽后一直用 Tail。</summary>
    private sealed class BrokerScript
    {
        public Queue<(int Code, int DelayMs)> Steps { get; } = new();
        public (int Code, int DelayMs) Tail { get; set; } = (ResponseCode.Success, 0);
        public int Requests;
        public readonly List<int> SendOpaques = new();
    }

    /// <summary>起 brokerCount 个真监听的 broker + 一个 namesrv。</summary>
    public static MockCluster Start(int brokerCount, bool routeOk = true) =>
        StartAt(Enumerable.Repeat<string?>(null, brokerCount).ToList(), routeOk);

    /// <summary>
    /// 按给定的地址表起集群：<b>null = 起一个真监听的 mock broker</b>，非 null 原样写进路由
    /// （塞 <see cref="DeadAddr" /> 就能造出「这台 broker 建连必败」）。broker 名仍是
    /// broker-0..N，所以 <see cref="Requests" /> 的下标与这里的顺序一致。
    /// </summary>
    public static MockCluster StartAt(List<string?> addrs, bool routeOk = true)
    {
        var listeners = new List<(int Index, Socket Socket)>();
        var final = new List<string>();
        for (int i = 0; i < addrs.Count; ++i)
        {
            if (addrs[i] is not null)
            {
                final.Add(addrs[i]!);
                continue;
            }

            Socket l = BindLoopback();
            listeners.Add((i, l));
            final.Add(EndPointOf(l));
        }

        MockCluster cluster = WithAddrs(final, routeOk);
        foreach ((int index, Socket socket) in listeners)
        {
            int broker = index;
            Serve(socket, req => cluster.BrokerRespond(broker, req));
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
            broker.SendOpaques.Clear();
        }
    }

    /// <summary>让第 index 个 broker 从此**收而不答**（把客户端逼到响应超时）。</summary>
    public void MakeSilent(int index) =>
        Script(index, new List<(int, int)>(), (NoReply, 0));

    /// <summary>第 index 个 broker 收到的 SEND 请求数。</summary>
    public int Requests(int index)
    {
        lock (_gate)
        {
            return _brokers[index].Requests;
        }
    }

    /// <summary>
    /// 第 index 个 broker 上第 n 笔（从 0 起）**发送**请求的 opaque；还没收到这么多笔时返回
    /// <see cref="int.MinValue" />。异步重试链要求每轮换新 opaque，并发发送要求两两不重复。
    /// </summary>
    public int SendOpaque(int index, int n)
    {
        lock (_gate)
        {
            List<int> opaques = _brokers[index].SendOpaques;
            return n >= 0 && n < opaques.Count ? opaques[n] : int.MinValue;
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

    /// <summary>第 n 笔（从 0 起）发送请求的取证，跨 broker 按到达顺序。</summary>
    public WireRecord? SendRequestAt(int n)
    {
        lock (_gate)
        {
            return _requests.Where(r => IsSendCode(r.Code)).ElementAtOrDefault(n);
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
            broker.SendOpaques.Add(req.Opaque);
        }

        if (delay > 0)
        {
            Thread.Sleep(delay);
        }

        if (req.IsOnewayRpc() || code == NoReply)
        {
            // 收而不答：客户端只能等到自己的响应超时（Java 的 scanResponseTable）
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

    internal static string BrokerName(int index) =>
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
    /// 一个绑好又立刻关掉的 loopback 端口：连它必然被拒（Java 的 RemotingConnectException），
    /// 而且**毫秒级**返回 —— 异步链的「建连失败要重试」只有靠它才不用等满超时。
    /// </summary>
    internal static string DeadAddr()
    {
        Socket probe = new(AddressFamily.InterNetwork, SocketType.Stream, ProtocolType.Tcp);
        probe.Bind(new IPEndPoint(IPAddress.Loopback, 0));
        string addr = ((IPEndPoint)probe.LocalEndPoint!).ToString();
        probe.Dispose();
        return addr;
    }

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
