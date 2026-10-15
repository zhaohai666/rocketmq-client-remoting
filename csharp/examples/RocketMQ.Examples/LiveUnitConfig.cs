// Real-machine integration of unitName / unitMode / enableStreamRequestType (U1-U5).
//
// Why it must be a real machine: these three switches can only be shown offline to "have the
// field filled in"; they cannot show whether the broker accepts them. The broker-side
// evidence is only these:
//   * a send carrying unitMode -> the topicSysFlag of the auto-created topic carries the
//     UNIT bit;
//   * a heartbeat carrying ConsumerData.unitMode -> the %RETRY%group that gets created
//     carries the UNIT_SUB bit;
//   * the @unitName/@STREAM suffix of the clientId shows up in the connection info the
//     broker records (examineConsumerConnectionInfo) -- this is the only observation point
//     that can prove "the one that went on the wire really is the assembled clientId".
// ReqT itself is inert for an ordinary broker (only the proxy / stream link reads it), so
// stream is only verified via the clientId here; the hook ordering (ReqT must land inside
// the ACL signature) is pinned by tests/AclTests.cs.
//
// Usage: rmq unit-config [namesrv] (needs a local 5.5.1 cluster, autoCreateTopicEnable=true)
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveUnitConfig
{
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

    /// <summary>A topic/group set isolated by stamp; Cleanup sweeps it afterwards.</summary>
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
    /// Configures and starts a producer. instanceName is given explicitly so that the
    /// clientId is predictable (the default name DEFAULT is rewritten in place to
    /// &lt;pid&gt;#&lt;nanoTime&gt;).
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
    /// An auto-created topic is only queryable for its route after the broker registers it
    /// with the namesrv; when wantBit is non-zero this waits for that bit to appear, and
    /// when it is 0 it only waits for the route to become visible (used for the "must not
    /// carry the unit bit" control).
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
                // not created yet, keep waiting
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
        Check("U1 sending still works with a unitName", r.SendStatus == SendStatus.SendOk, r.MsgId);

        using ScopedProducer ctl = new("u1ctl");
        Check("U1 with no unitName set the clientId gains no segment",
            ctl.ClientId == MixAll.CachedIpStr() + "@uc-dotnet-u1ctl-" + _stamp,
            "actual=" + ctl.ClientId);
    }

    // ---------------------------------------------------------------- U2 the clientId of a stream consumer
    private static void U2StreamConsumerIsVisibleOnBroker()
    {
        string t = Topic("U2");
        string g = Group("u2");
        // warm one up first: the topic gets created and the route becomes visible, so the consumer's first pull does not hit TOPIC_NOT_EXIST
        using (ScopedProducer warm = new("u2warm"))
        {
            warm.Send(t, "warm");
            Check("U2 the warm-up message's route is visible", RouteVisible(t, 20));
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
        Check("U2 the consumer's clientId ends with @unitA@STREAM", c.ClientId.EndsWith(wantSuffix),
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
        Check("U2 the stream consumer received the message", HasBody(collector.Snapshot(), "hello-stream"),
            Join(collector.Snapshot()));

        // the broker-side connection info is the only observation point that can prove "the clientId that went on the wire is the assembled one"
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
        Check("U2 the clientId the broker recorded also carries @unitA@STREAM", seenOnBroker, observed);
        c.Shutdown();
    }

    // ---------------------------------------------------------------- U3 a unitMode send -> the UNIT bit
    private static void U3UnitModeSendMarksTopic()
    {
        string on = Topic("U3On");
        string off = Topic("U3Off");
        using (ScopedProducer p = new("u3on", unitMode: true))
        {
            SendResult r = p.Send(on, "unit-on");
            Check("U3 the unitMode send succeeded", r.SendStatus == SendStatus.SendOk, r.MsgId);
        }
        using (ScopedProducer p = new("u3off"))
        {
            SendResult r = p.Send(off, "unit-off");
            Check("U3 the control send succeeded", r.SendStatus == SendStatus.SendOk, r.MsgId);
        }

        int onFlag = WaitSysFlag(on, FlagUnit, 30);
        Check("U3 a topic created with unitMode=true carries the UNIT bit", (onFlag & FlagUnit) != 0,
            "sysFlag=" + onFlag);
        int offFlag = WaitSysFlag(off, 0, 30);
        Check("U3 a topic created with unitMode=false carries no unit bit", (offFlag & FlagUnit) == 0,
            "sysFlag=" + offFlag);
    }

    // ---------------------------------------------------------------- U4 heartbeat unitMode -> %RETRY% UNIT_SUB
    private static void U4UnitModeConsumerMarksRetryTopic()
    {
        string t = Topic("U4");
        string g = Group("u4");
        using (ScopedProducer warm = new("u4warm"))
        {
            warm.Send(t, "warm");
            Check("U4 the warm-up message's route is visible", RouteVisible(t, 20));
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

        // %RETRY%<group> is created by the broker while handling the heartbeat, based on ConsumerData.unitMode
        int flag = WaitSysFlag(MixAll.GetRetryTopic(g), FlagUnitSub, 40);
        Check("U4 a heartbeat with unitMode=true makes %RETRY% carry the UNIT_SUB bit", (flag & FlagUnitSub) != 0,
            "sysFlag=" + flag);
        c.Shutdown();
    }

    // ---------------------------------------------------------------- U5 a stream producer + a lite consumer
    private static void U5StreamProducerAndLiteConsumer()
    {
        string t = Topic("U5");
        string g = Group("u5");

        var lite = new DefaultLitePullConsumer(g);
        lite.SetInstanceName("uc-dotnet-u5-" + _stamp);
        lite.SetNamesrvAddr(_namesrv);
        lite.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        lite.SetPollTimeoutMillis(1000);
        lite.Subscribe(t, "*");
        lite.Start();
        Check("U5 a lite consumer's default clientId carries @STREAM", lite.ClientId.EndsWith(StreamSuffix),
            "actual=" + lite.ClientId);

        using ScopedProducer p = new("u5", stream: true);
        Check("U5 a producer with stream explicitly enabled has a clientId carrying @STREAM",
            p.ClientId.EndsWith(StreamSuffix), "actual=" + p.ClientId);
        for (int i = 0; i < 3; ++i)
        {
            SendResult r = p.Send(t, "m" + i);
            Check("U5 message number " + i + " was sent successfully", r.SendStatus == SendStatus.SendOk, r.MsgId);
        }

        // The assignment must only be waited for **after the first message**: the topic is
        // auto-created by the first send and the namesrv had no route before that, so a
        // rebalance getting no queues is correct behaviour (not a client bug).
        for (int i = 0; i < 40 && lite.Assignment().Count == 0; ++i)
        {
            Sleep(500);
        }
        Check("U5 the lite consumer got a queue assignment", lite.Assignment().Count > 0,
            Num(lite.Assignment().Count));

        List<string> got = new();
        DateTime deadline = DateTime.UtcNow.AddSeconds(30);
        while (got.Count < 3 && DateTime.UtcNow < deadline)
        {
            // poll drains the local buffer, so it has to be accumulated across several polls to collect all 3
            foreach (MessageExt m in lite.Poll(1000)) got.Add(Str(m.Body));
        }
        Check("U5 the lite consumer received all 3",
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
            // %RETRY%/%DLQ% go away together with the subscription group (the broker really deletes the retry topic only when the delete-group endpoint is used)
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
                // the retry topic is already gone when the group is deleted
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
            Check("the integration threw", false, e.Message);
        }

        Cleanup();
        _admin.Shutdown();
        Console.WriteLine();
        Console.WriteLine("PASS=" + Volatile.Read(ref _pass) + " FAIL=" + Volatile.Read(ref _fail));
        return Volatile.Read(ref _fail) == 0 ? 0 : 1;
    }
}
