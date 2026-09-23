// 生产者退出时的 UNREGISTER_CLIENT(35) 真机验证（.NET）。
// 用法：rmq unreg-live [namesrv]
//
// 与 python/verify_producer_unregister_live.py（U1~U6）、
// cpp/examples/live_producer_unregister.cpp、rust/examples/live_producer.rs 的 P11 同一套场景。
//
// Java 的 `DefaultMQProducerImpl#shutdown`:313 调 `mQClientFactory.unregisterProducer(group)`
// （`MQClientInstance`:1198-1201），后者进私有的 `unregisterClient(producerGroup, null)`:1158-1182：
// 给 `brokerAddrTable` 里**每台 broker（含 slave）**同步发一发 code 35，超时
// `getMqClientApiTimeout()`（3000ms），任何异常只 `log.warn`。
// `MQClientAPIImpl#unregisterClient`:1615-1639 组的头是
// `UnregisterClientRequestHeader{clientID, producerGroup, consumerGroup}` —— 键名是大写 ID 的
// `clientID`，生产者退出时 `consumerGroup` 传 null（**整字段不上线**）。
// broker 端 `ClientManageProcessor#unregisterClient`:213-249 判的是 `group != null`：
// 空串会被当成「真有个空组名」去查 `""` 的订阅组配置，所以这里必须盯住字段有没有上线，
// 而不是只盯值。
//
// 本脚本证四件事：
//   U1  生产者起来并真的发了消息（组注册的前置条件）。
//   U2  204 `GET_PRODUCER_CONNECTION_LIST` 能看到本 clientId —— 注册确实发生过，
//       「消失」才有意义。注册靠心跳上线（30s 一轮），所以要轮询等。
//   U3  `Shutdown()` 期间钩子抓到 code 35：每台已知 broker 各一发，头是
//       clientID + producerGroup，`consumerGroup` 不上线，且排在业务发送之后。
//   U5  紧接着查 204：这个组已经不在了（broker 回 SYSTEM_ERROR
//       `the producer group[...] not exist`，Java 的 mqadmin 也这么判）。
//   U6  对照组（另一个没退出的生产者组）仍在 —— 排掉「broker 把所有连接都清了」这种假阳性。
//
// ⚠ 判据强度：.NET 里每个生产者各自持有一份 `MQClientInstance`、各自一条 TCP 连接，退出时
// 连接也会关掉，broker 的通道扫描同样会把组摘掉 —— 单看 U5 分不出是 35 还是断连的功劳，
// 所以这里必须由钩子抓帧（U3）直接证明「线上走了这一发」。行为级的判别式证明在
// `rust/examples/live_producer.rs` 的 P11：Rust 按 clientId 复用实例，两个同 instanceName、
// 不同组的生产者共用一条连接，先退的那个连接还活着，组能消失只可能是因为 35。
// 另：「每一发 35 都回 SUCCESS」在本移植**不可观测** —— 传输层有意不调
// `IRpcHook#DoAfterResponse`（见 Remoting/RemotingClient.cs 的说明），
// U5 的 broker 侧效果就是它的替代判据。头形状、从节点扇出、单台失败被吞这三条
// 在 `tests/RocketMQ.Client.Tests/ProducerUnregisterTests.cs` 里离线锁死。
//
// 前置：NameServer + Broker 已起，``autoCreateTopicEnable=true``。
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class ProducerUnregisterLive
{
    private static readonly string Stamp =
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    private static int _pass;
    private static int _fail;

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok)
        {
            _pass++;
            Console.WriteLine("  [PASS] " + name + (detail.Length == 0 ? "" : "  " + detail));
        }
        else
        {
            _fail++;
            Console.WriteLine("  [FAIL] " + name + (detail.Length == 0 ? "" : "  " + detail));
        }
    }

    private static string Num(int v) => v.ToString(CultureInfo.InvariantCulture);

    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 200)
    {
        long deadline = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + timeoutMs;
        while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(intervalMs);
        }

        return pred();
    }

    private static string Join(IEnumerable<string> v) => string.Join(",", v);

    /// <summary>
    /// 逐帧记录上线请求的探针。钩子跑在 encode() **之前**，此时头还挂在 CustomHeader 上
    /// （`MakeCustomHeaderToNet` 是编码阶段的事，与 Java 同一时点），所以取它的 ToExtFields()。
    /// </summary>
    private sealed class UnregisterProbe : IRpcHook
    {
        private readonly object _lk = new();
        private readonly List<Frame> _frames = new();
        private int _count;

        public sealed class Frame
        {
            public int Seq { get; init; }
            public int Code { get; init; }
            public string Addr { get; init; } = string.Empty;
            public PropertyMap Ext { get; init; } = new();
        }

        public void DoBeforeRequest(string remoteAddr, RemotingCommand request)
        {
            PropertyMap ext = request.CustomHeader is not null
                ? new PropertyMap(request.CustomHeader.ToExtFields())
                : new PropertyMap(request.ExtFields);
            lock (_lk)
            {
                _frames.Add(new Frame
                {
                    Seq = ++_count,
                    Code = request.Code,
                    Addr = remoteAddr,
                    Ext = ext,
                });
            }
        }

        /// <summary>本端口的传输层有意不调这一句（见文件头说明），所以永远不会被触发。</summary>
        public void DoAfterResponse(string remoteAddr, RemotingCommand request,
            RemotingCommand? response)
        {
            _ = remoteAddr;
            _ = request;
            _ = response;
        }

        public List<Frame> Frames()
        {
            lock (_lk) return new List<Frame>(_frames);
        }

        public List<Frame> Of(int code) => Frames().Where(f => f.Code == code).ToList();

        /// <summary>最后一条业务发送的序号（用来判「注销排在发送之后」）。</summary>
        public int LastOf(params int[] codes)
        {
            int seq = -1;
            foreach (Frame f in Frames())
            {
                if (codes.Contains(f.Code)) seq = f.Seq;
            }

            return seq;
        }
    }

    private static string ExtField(UnregisterProbe.Frame f, string key) =>
        f.Ext.TryGetValue(key, out string? v) ? v : string.Empty;

    private static string ExtText(PropertyMap ext) =>
        "{" + string.Join(", ", ext.Select(kv => kv.Key + "=" + kv.Value)) + "}";

    /// <summary>
    /// 204 看到的 clientId 列表；broker 说「组不存在」时返回空表
    /// （Java 的 mqadmin 同样把 SYSTEM_ERROR 当「不在线」）。
    /// </summary>
    private static List<string> ConnectionClientIds(DefaultMQAdminExt admin, string addr,
        string group)
    {
        try
        {
            ProducerConnection pc = admin.ExamineProducerConnectionInfo(group, addr);
            return pc.ConnectionSet.Select(c => c.ClientId).ToList();
        }
        catch (Exception e)
        {
            string msg = e.Message;
            if (msg.Contains("not exist", StringComparison.Ordinal)
                || msg.Contains("not online", StringComparison.Ordinal))
            {
                return new List<string>();
            }

            Console.WriteLine("  [diag] ExamineProducerConnectionInfo(" + group + ") 异常: " + msg);
            return new List<string>();
        }
    }

    private sealed class ScopedProducer : IDisposable
    {
        public DefaultMQProducer Producer { get; }

        public ScopedProducer(string group, string namesrv, string kind, string stamp,
            IRpcHook? hook)
        {
            Producer = new DefaultMQProducer(group)
            {
                NamesrvAddr = namesrv,
                InstanceName = kind + "-" + stamp,
                SendMsgTimeout = 5000,
            };
            if (hook is not null) Producer.SetRpcHook(hook);
            Producer.Start();
        }

        public void Dispose()
        {
            try
            {
                Producer.Shutdown();
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] producer shutdown failed: " + e.Message);
            }
        }
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        var admin = new DefaultMQAdminExt("UNREGADMIN");
        admin.SetNamesrvAddr(namesrv);
        admin.SetTimeoutMillis(10000);
        admin.Start();

        try
        {
            // 已知 broker：主从都算，35 必须每台各一发（Java 遍历的是每个 brokerId）
            ClusterInfo cluster = new();
            for (int i = 0; i < 40; ++i)
            {
                try
                {
                    cluster = admin.FetchBrokerClusterInfo();
                    if (cluster.BrokerAddrTable.Count > 0) break;
                }
                catch (Exception e)
                {
                    if (i == 39) Console.WriteLine("  [diag] cluster probe failed: " + e.Message);
                }

                Thread.Sleep(1000);
            }

            if (cluster.BrokerAddrTable.Count == 0)
            {
                Check("集群探活", false, "nameServer 无 broker 注册");
                return 1;
            }

            List<string> brokers = cluster.GetBrokerAddrs();
            Check("集群探活", true, "brokers=" + Join(brokers));
            RunChecks(namesrv, brokers, admin);
            Console.WriteLine();
            Console.WriteLine("== 结果: " + Num(_pass) + "/" + Num(_pass + _fail) + " 通过 ==");
            return _fail == 0 ? 0 : 1;
        }
        finally
        {
            try
            {
                admin.Shutdown();
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] admin shutdown failed: " + e.Message);
            }
        }
    }

    private static void RunChecks(string namesrv, List<string> brokers, DefaultMQAdminExt admin)
    {
        string brokerAddr = brokers[0];
        string topic = "Unreg_" + Stamp;
        string group = "PID_unreg_" + Stamp;
        string peerGroup = "PID_unreg_peer_" + Stamp;

        var probe = new UnregisterProbe();
        using var p = new ScopedProducer(group, namesrv, "unreg", Stamp, probe);
        using var peer = new ScopedProducer(peerGroup, namesrv, "unreg-peer", Stamp, null);

        SendResult r = p.Producer.Send(new Message(topic, Encoding.UTF8.GetBytes("unreg-probe")),
            5000);
        Check("U1 生产者发送成功", r.SendStatus == SendStatus.SendOk, "msgId=" + r.MsgId);
        peer.Producer.Send(new Message(topic, Encoding.UTF8.GetBytes("peer")), 5000);

        string clientId = p.Producer.ClientId;
        List<string> seen = new();
        bool registered = WaitUntil(() =>
        {
            seen = ConnectionClientIds(admin, brokerAddr, group);
            return seen.Contains(clientId);
        }, 70000);
        Check("U2 心跳后 204 能看到本 clientId", registered,
            "clientId=" + clientId + " 当前=" + Join(seen));
        List<string> peerSeen = ConnectionClientIds(admin, brokerAddr, peerGroup);
        Check("U2b 对照组注册可见（204 这条判据本身有效）",
            peerSeen.Contains(peer.Producer.ClientId), "当前=" + Join(peerSeen));

        Check("U3 前置：还没退出时不应有 35", probe.Of(RequestCode.UnregisterClient).Count == 0,
            "count=" + Num(probe.Of(RequestCode.UnregisterClient).Count));

        // 注销走的是**还开着**的那条长连接：.NET 的 Shutdown 顺序是先 35 再关客户端，
        // 这条顺序只有 broker 侧能验证（U5）。
        p.Producer.Shutdown();

        List<UnregisterProbe.Frame> unregs = probe.Of(RequestCode.UnregisterClient);
        Check("U3 Shutdown 给每台已知 broker 各发了一发 35",
            unregs.Count == brokers.Count,
            "count=" + Num(unregs.Count) + " brokers=" + Num(brokers.Count));
        bool shapeOk = unregs.Count > 0;
        foreach (UnregisterProbe.Frame f in unregs)
        {
            if (ExtField(f, "clientID") != clientId) shapeOk = false;
            if (ExtField(f, "producerGroup") != group) shapeOk = false;
            // Java 的 unregisterClient(group, null)：消费者槽位整个不上线
            if (f.Ext.ContainsKey("consumerGroup")) shapeOk = false;
            if (!brokers.Contains(f.Addr)) shapeOk = false;
        }

        Check("U3b 35 的头是 clientID + producerGroup，consumerGroup 不上线（Java 传 null）",
            shapeOk, unregs.Count == 0
                ? "没抓到帧"
                : ("addr=" + unregs[0].Addr + " ext=" + ExtText(unregs[0].Ext)));
        int lastSend = probe.LastOf(RequestCode.SendMessage, RequestCode.SendMessageV2);
        int firstUnreg = unregs.Count == 0 ? -1 : unregs[0].Seq;
        Check("U3c 35 排在业务发送之后", lastSend >= 0 && firstUnreg > lastSend,
            "last_send=" + Num(lastSend) + " first_unreg=" + Num(firstUnreg));

        bool gone = WaitUntil(() => ConnectionClientIds(admin, brokerAddr, group).Count == 0, 10000);
        Check("U5 退出后 204 查不到这个组（broker 回 not exist）", gone,
            "当前=" + Join(ConnectionClientIds(admin, brokerAddr, group)));

        List<string> peerAfter = ConnectionClientIds(admin, brokerAddr, peerGroup);
        Check("U6 对照组仍在（排掉「broker 全清」这种假阳性）",
            peerAfter.Contains(peer.Producer.ClientId), "当前=" + Join(peerAfter));
    }
}
