// unitName / unitMode / enableStreamRequestType 真机联调
//（对应 python/verify_unit_config_live.py、cpp/examples/live_unit_config.cpp、
//  rust/examples/live_unit_config.rs 的 U1–U5）。
//
// 为什么必须真机：这三个开关在离线单测里只能证明「字段被填了」，证明不了 broker 认不认。
// broker 侧可见的证据只有这几处：
//   * 发送带 unitMode → 自动建出来的 topic 的 topicSysFlag 带 UNIT 位
//     （AbstractSendMessageProcessor.java:487-497）；
//   * 心跳带 ConsumerData.unitMode → %RETRY%group 建出来带 UNIT_SUB 位
//     （ClientManageProcessor.java:113-118）；
//   * clientId 的 @unitName/@STREAM 后缀会出现在 broker 记录的连接信息里
//     （examineConsumerConnectionInfo）—— 这是唯一能证明「上线的确实是拼好的那个
//     clientId」的观测点。
// ReqT 本身对普通 broker 是惰性的（只有 proxy/stream 链路读它），所以 stream 在这里只验
// clientId；钩子顺序（ReqT 必须落在 ACL 签名之内）由 tests/AclTests.cs 锁死。
//
// 用法：rmq unit-config [namesrv]（需本地 5.5.1 集群，autoCreateTopicEnable=true）
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveUnitConfig
{
    // TopicSysFlag 的两个单元位（Java TopicSysFlag：FLAG_UNIT=0x1、FLAG_UNIT_SUB=0x2）
    private const int FlagUnit = 0x1;
    private const int FlagUnitSub = 0x2;
    private const string StreamSuffix = "@STREAM";

    private static string _namesrv = "127.0.0.1:9876";
    private static string _stamp = string.Empty;
    private static DefaultMQAdminExt _admin = null!;

    private static int _pass;
    private static int _fail;

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok)
        {
            Interlocked.Increment(ref _pass);
            Console.WriteLine("  [PASS] " + name + (detail.Length > 0 ? "  " + detail : ""));
        }
        else
        {
            Interlocked.Increment(ref _fail);
            Console.WriteLine("  [FAIL] " + name + (detail.Length > 0 ? "  " + detail : ""));
        }
    }

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string Str(byte[] body) => Encoding.UTF8.GetString(body);

    private static string Join(IEnumerable<string> items) => "[" + string.Join(",", items) + "]";

    private static bool HasBody(List<string> bodies, string want) => bodies.Contains(want);

    private static void Sleep(int ms) => Thread.Sleep(ms);

    /// <summary>按 stamp 隔离的一套 topic/group；跑完 Cleanup 扫干净。</summary>
    private static readonly List<string> Topics = new();

    private static readonly List<string> Groups = new();

    private static string Topic(string kind)
    {
        string t = "UnitDotnet_" + _stamp + "_" + kind;
        Topics.Add(t);
        return t;
    }

    private static string Group(string kind)
    {
        string g = "GID_unit_dotnet_" + _stamp + "_" + kind;
        Groups.Add(g);
        return g;
    }

    private static string BrokerAddr()
    {
        try
        {
            TopicRouteData route = _admin.ExamineTopicRoute(MixAll.DefaultTopic);
            if (route.BrokerDatas.Count > 0)
            {
                string addr = route.BrokerDatas[0].SelectBrokerAddr();
                if (addr.Length > 0) return addr;
            }
        }
        catch (Exception e)
        {
            Console.WriteLine("  [diag] broker route failed: " + e.Message);
        }
        return "127.0.0.1:10911";
    }

    private sealed class Collector : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<string> _bodies = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk) foreach (MessageExt m in msgs) _bodies.Add(Str(m.Body));
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public List<string> Snapshot()
        {
            lock (_lk) return new List<string>(_bodies);
        }
    }

    /// <summary>
    /// 配好并启动一个生产者。instanceName 显式给定，clientId 才可预测
    ///（默认名 DEFAULT 会被就地改写成 &lt;pid&gt;#&lt;nanoTime&gt;）。
    /// </summary>
    private sealed class ScopedProducer : IDisposable
    {
        private readonly DefaultMQProducer _producer;

        public ScopedProducer(string kind, string unitName = "", bool unitMode = false,
            bool stream = false)
        {
            _producer = new DefaultMQProducer("GID_unit_dotnet_" + _stamp + "_" + kind + "_pg")
            {
                NamesrvAddr = _namesrv,
                InstanceName = "uc-dotnet-" + kind + "-" + _stamp,
                SendMsgTimeout = 5000,
                UnitName = unitName,
                UnitMode = unitMode,
                EnableStreamRequestType = stream,
            };
            _producer.Start();
        }

        public string ClientId => _producer.ClientId;

        public SendResult Send(string topic, string body) =>
            _producer.Send(new Message(topic, Bytes(body)));

        public void Dispose()
        {
            try
            {
                _producer.Shutdown();
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] producer shutdown failed: " + e.Message);
            }
        }
    }

    /// <summary>
    /// 自动建出来的 topic 要先经 broker→namesrv 注册才查得到路由；wantBit 非 0 时等到该位
    /// 出现为止，为 0 时只等路由可见（用于「不该带单元位」的对照）。
    /// </summary>
    private static int WaitSysFlag(string topic, int wantBit, int seconds)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(seconds);
        int last = -1;
        for (;;)
        {
            List<QueueData> queues = new();
            try
            {
                queues = _admin.ExamineTopicRoute(topic).QueueDatas;
            }
            catch (Exception)
            {
                // 还没建出来，继续等
            }
            if (queues.Count > 0)
            {
                last = queues[0].TopicSysFlag;
                if (wantBit == 0 || (last & wantBit) != 0) return last;
            }
            if (DateTime.UtcNow >= deadline) return last;
            Sleep(500);
        }
    }

    private static bool RouteVisible(string topic, int seconds) => WaitSysFlag(topic, 0, seconds) >= 0;

    // ---------------------------------------------------------------- U1 unitName → clientId
    private static void U1ClientIdCarriesUnitName()
    {
        using ScopedProducer p = new("u1", "unitA");
        string want = MixAll.CachedIpStr() + "@uc-dotnet-u1-" + _stamp + "@unitA";
        Check("U1 clientId = ip@instanceName@unitName", p.ClientId == want,
            "actual=" + p.ClientId + " want=" + want);
        SendResult r = p.Send(Topic("U1"), "u1");
        Check("U1 带 unitName 仍能正常发送", r.SendStatus == SendStatus.SendOk, r.MsgId);

        using ScopedProducer ctl = new("u1ctl");
        Check("U1 不设 unitName 时 clientId 不多出段",
            ctl.ClientId == MixAll.CachedIpStr() + "@uc-dotnet-u1ctl-" + _stamp,
            "actual=" + ctl.ClientId);
    }

    // ---------------------------------------------------------------- U2 stream 消费者的 clientId
    private static void U2StreamConsumerIsVisibleOnBroker()
    {
        string t = Topic("U2");
        string g = Group("u2");
        // 先暖一条：topic 建出来 + 路由可见，消费者首 pull 才不会撞 TOPIC_NOT_EXIST
        using (ScopedProducer warm = new("u2warm"))
        {
            warm.Send(t, "warm");
            Check("U2 预热消息的路由已可见", RouteVisible(t, 20));
        }

        Collector collector = new();
        var c = new DefaultMQPushConsumer(g)
        {
            InstanceName = "uc-dotnet-u2-" + _stamp,
            UnitName = "unitA",
            EnableStreamRequestType = true,
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        c.SetNamesrvAddr(_namesrv);
        c.SetMessageListener(collector);
        c.Subscribe(t, "*");
        c.Start();

        string wantSuffix = "@unitA" + StreamSuffix;
        Check("U2 消费者 clientId 以 @unitA@STREAM 收尾", c.ClientId.EndsWith(wantSuffix),
            "actual=" + c.ClientId);

        using (ScopedProducer p = new("u2send"))
        {
            p.Send(t, "hello-stream");
        }

        DateTime deadline = DateTime.UtcNow.AddSeconds(30);
        while (!HasBody(collector.Snapshot(), "hello-stream") && DateTime.UtcNow < deadline)
        {
            Sleep(500);
        }
        Check("U2 stream 消费者收到消息", HasBody(collector.Snapshot(), "hello-stream"),
            Join(collector.Snapshot()));

        // broker 侧连接信息是唯一能证明「上线 clientId 就是拼好的那个」的观测点
        bool seenOnBroker = false;
        string observed = string.Empty;
        for (int i = 0; i < 20 && !seenOnBroker; ++i)
        {
            observed = string.Empty;
            try
            {
                ConsumerConnection conn = _admin.ExamineConsumerConnectionInfo(g);
                foreach (Connection cn in conn.ConnectionSet)
                {
                    observed += (observed.Length == 0 ? "" : ",") + cn.ClientId;
                    if (cn.ClientId.EndsWith(wantSuffix)) seenOnBroker = true;
                }
            }
            catch (Exception e)
            {
                observed = "examine failed: " + e.Message;
            }
            if (!seenOnBroker) Sleep(500);
        }
        Check("U2 broker 记录的 clientId 也带 @unitA@STREAM", seenOnBroker, observed);
        c.Shutdown();
    }

    // ---------------------------------------------------------------- U3 unitMode 发送 → UNIT 位
    private static void U3UnitModeSendMarksTopic()
    {
        string on = Topic("U3On");
        string off = Topic("U3Off");
        using (ScopedProducer p = new("u3on", unitMode: true))
        {
            SendResult r = p.Send(on, "unit-on");
            Check("U3 unitMode 发送成功", r.SendStatus == SendStatus.SendOk, r.MsgId);
        }
        using (ScopedProducer p = new("u3off"))
        {
            SendResult r = p.Send(off, "unit-off");
            Check("U3 对照发送成功", r.SendStatus == SendStatus.SendOk, r.MsgId);
        }

        int onFlag = WaitSysFlag(on, FlagUnit, 30);
        Check("U3 unitMode=true 建出的 topic 带 UNIT 位", (onFlag & FlagUnit) != 0,
            "sysFlag=" + onFlag);
        int offFlag = WaitSysFlag(off, 0, 30);
        Check("U3 unitMode=false 建出的 topic 不带单元位", (offFlag & FlagUnit) == 0,
            "sysFlag=" + offFlag);
    }

    // ---------------------------------------------------------------- U4 心跳 unitMode → %RETRY% UNIT_SUB
    private static void U4UnitModeConsumerMarksRetryTopic()
    {
        string t = Topic("U4");
        string g = Group("u4");
        using (ScopedProducer warm = new("u4warm"))
        {
            warm.Send(t, "warm");
            Check("U4 预热消息的路由已可见", RouteVisible(t, 20));
        }

        var c = new DefaultMQPushConsumer(g)
        {
            InstanceName = "uc-dotnet-u4-" + _stamp,
            UnitMode = true,
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        c.SetNamesrvAddr(_namesrv);
        c.SetMessageListener(new Collector());
        c.Subscribe(t, "*");
        c.Start();

        // %RETRY%<group> 由 broker 处理心跳时按 ConsumerData.unitMode 建出来
        int flag = WaitSysFlag(MixAll.GetRetryTopic(g), FlagUnitSub, 40);
        Check("U4 心跳 unitMode=true 让 %RETRY% 带 UNIT_SUB 位", (flag & FlagUnitSub) != 0,
            "sysFlag=" + flag);
        c.Shutdown();
    }

    // ---------------------------------------------------------------- U5 stream 生产者 + lite 消费者
    private static void U5StreamProducerAndLiteConsumer()
    {
        string t = Topic("U5");
        string g = Group("u5");

        // 不动 EnableStreamRequestType：Java 在 DefaultLitePullConsumer 的构造函数里置 true，
        // 这里要验的就是「默认值真的生效」。
        var lite = new DefaultLitePullConsumer(g);
        lite.SetInstanceName("uc-dotnet-u5-" + _stamp);
        lite.SetNamesrvAddr(_namesrv);
        lite.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        lite.SetPollTimeoutMillis(1000);
        lite.Subscribe(t, "*");
        lite.Start();
        // Java 在 DefaultLitePullConsumer 的每个构造函数里置 true ⇒ 默认就该带 @STREAM
        Check("U5 lite 消费者默认 clientId 带 @STREAM", lite.ClientId.EndsWith(StreamSuffix),
            "actual=" + lite.ClientId);

        using ScopedProducer p = new("u5", stream: true);
        Check("U5 显式开 stream 的生产者 clientId 带 @STREAM",
            p.ClientId.EndsWith(StreamSuffix), "actual=" + p.ClientId);
        for (int i = 0; i < 3; ++i)
        {
            SendResult r = p.Send(t, "m" + i);
            Check("U5 第 " + i + " 条发送成功", r.SendStatus == SendStatus.SendOk, r.MsgId);
        }

        // 分配必须在**首条消息之后**才等：topic 由第一次发送自动建出来，之前 namesrv 没有
        // 路由，rebalance 拿到空队列是正确行为（不是客户端 bug）。
        for (int i = 0; i < 40 && lite.Assignment().Count == 0; ++i)
        {
            Sleep(500);
        }
        Check("U5 lite 消费者拿到队列分配", lite.Assignment().Count > 0,
            Num(lite.Assignment().Count));

        List<string> got = new();
        DateTime deadline = DateTime.UtcNow.AddSeconds(30);
        while (got.Count < 3 && DateTime.UtcNow < deadline)
        {
            // poll 会掏空本地缓冲，必须跨多次 poll 累加才凑得齐 3 条
            foreach (MessageExt m in lite.Poll(1000)) got.Add(Str(m.Body));
        }
        Check("U5 lite 消费者收到全部 3 条",
            HasBody(got, "m0") && HasBody(got, "m1") && HasBody(got, "m2"), Join(got));
        lite.Shutdown();
    }

    private static string Num(int v) => v.ToString(CultureInfo.InvariantCulture);

    private static void Cleanup()
    {
        string addr = BrokerAddr();
        foreach (string t in Topics)
        {
            try
            {
                _admin.DeleteTopic(t);
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] DeleteTopic(" + t + ") failed: " + e.Message);
            }
        }
        foreach (string g in Groups)
        {
            // %RETRY%/%DLQ% 随订阅组一起删（只有走删组接口 broker 才会真删 retry topic）
            try
            {
                _admin.DeleteSubscriptionGroup(addr, g, true);
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] DeleteSubscriptionGroup(" + g + ") failed: " + e.Message);
            }
            try
            {
                _admin.DeleteTopic(MixAll.GetRetryTopic(g));
            }
            catch (Exception)
            {
                // 组删掉时 retry topic 已随之消失
            }
        }
    }

    public static int Run(string[] args)
    {
        _namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        _stamp = Num((int)(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() % 1000000));
        Console.WriteLine("namesrv=" + _namesrv + " stamp=" + _stamp
            + " clientIdIp=" + MixAll.CachedIpStr());

        _admin = new DefaultMQAdminExt("UCDOTNETADMIN");
        _admin.SetNamesrvAddr(_namesrv);
        _admin.Start();

        try
        {
            U1ClientIdCarriesUnitName();
            U2StreamConsumerIsVisibleOnBroker();
            U3UnitModeSendMarksTopic();
            U4UnitModeConsumerMarksRetryTopic();
            U5StreamProducerAndLiteConsumer();
        }
        catch (Exception e)
        {
            Check("联调异常", false, e.Message);
        }

        Cleanup();
        _admin.Shutdown();
        Console.WriteLine();
        Console.WriteLine("PASS=" + Volatile.Read(ref _pass) + " FAIL=" + Volatile.Read(ref _fail));
        return Volatile.Read(ref _fail) == 0 ? 0 : 1;
    }
}
