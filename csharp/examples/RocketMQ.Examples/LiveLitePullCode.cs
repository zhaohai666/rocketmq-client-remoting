// Real-machine verification of the lite-pull **request code / broker switch** (#107).
//
// Why it must be a real machine: the `FLAG_LITE_PULL_MESSAGE(0x10)` +
// `LITE_PULL_MESSAGE(361)` chain is always green against an offline fake broker -- with the
// bit missing or the code still 11, the request is still a perfectly legal pull and the fake
// broker (like a real broker's ordinary pull branch) replies with messages as usual. The only
// thing that can tell them apart is a real broker's `litePullMessageEnable` switch
// (`PullMessageProcessor:325-331` **only intercepts 361**): flip the switch to false at
// runtime (UPDATE_BROKER_CONFIG, no restart needed):
//
//   S1 the switch defaults to true: the whole lite pull chain works (the baseline).
//   S2 the switch is false:
//      S2a a bare 361 request -> NO_PERMISSION(16) + "...for lite pull consumer is forbidden";
//      S2b a bare 11 request on the same queue at the same offset -> SUCCESS and messages as
//    usual (**the control**: the switch only covers lite, an ordinary pull is unaffected --
//    without this leg S2a's failure could just mean the broker is broken);
//      S2c the lite consumer quietly starves: the message is right there but poll never
//          returns anything and the pull cursor does not move at all (the old implementation,
//       which did not set the bit / used code 11, would receive the message on this leg -> the
//          discriminator goes red);
//      S2d a push consumer consumes as usual (**the control**: the whole consume chain is not
//          broken).
//   S3 the switch is set back to true: lite pull recovers immediately.
//
// Before exiting it **unconditionally** sets `litePullMessageEnable` back to its original
// value (the same as with reset-offset).
//
// Usage: rmq lite-pull-code [namesrv]
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveLitePullCode
{
    private const string KConfigKey = "litePullMessageEnable";
    private const string KDenyRemark = "for lite pull consumer is forbidden";

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

    private static string N(long v) => v.ToString(CultureInfo.InvariantCulture);

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static string Join(IEnumerable<string> items) => "[" + string.Join(", ", items) + "]";

    private static int LiteFlag() => PullSysFlag.BuildSysFlag(commitOffset: false, suspend: false,
        subscription: true, classFilter: false, litePull: true);

   /// <summary>The 4-arg version at DefaultMQPullConsumerImpl.pullSyncImpl:248; the lite bit
    /// must be 0.</summary>
    private static int ClassicFlag() => PullSysFlag.BuildSysFlag(commitOffset: false, suspend: false,
        subscription: true, classFilter: false);

    private static string ReadFlag(DefaultMQAdminExt admin, string brokerAddr)
    {
        try
        {
            PropertyMap cfg = admin.GetBrokerConfig(brokerAddr);
            return cfg.TryGetValue(KConfigKey, out string? v) && v != null ? v : "<missing>";
        }
        catch (Exception e)
        {
            Console.WriteLine("  [diag] getBrokerConfig failed: " + e.Message);
            return "<error>";
        }
    }

    private static bool WriteFlag(DefaultMQAdminExt admin, string brokerAddr, string value)
    {
        try
        {
            var props = new PropertyMap { [KConfigKey] = value };
            admin.UpdateBrokerConfig(brokerAddr, props);
            return true;
        }
        catch (Exception e)
        {
            Console.WriteLine("  [diag] updateBrokerConfig(" + KConfigKey + "=" + value
                + ") failed: " + e.Message);
            return false;
        }
    }

    private static DefaultLitePullConsumer NewLite(string namesrv, string group,
        MessageQueue mq, string stamp)
    {
        var c = new DefaultLitePullConsumer(group);
        c.SetNamesrvAddr(namesrv);
        c.SetInstanceName("lite-code-net-" + stamp);
        c.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        // The case looks at the offsets itself: with auto-commit off, S2c's "the cursor does
        // not move at all" stays clean.
        c.SetAutoCommit(false);
        c.SetPullIntervalMillis(200);
        c.Assign(new[] { mq });
        c.Start();
        return c;
    }

    private static List<string> Drain(DefaultLitePullConsumer c, int expect, int timeoutMs)
    {
        var outBodies = new List<string>();
        long deadline = NowMs() + timeoutMs;
        while (outBodies.Count < expect && NowMs() < deadline)
        {
            foreach (MessageExt m in c.Poll(500))
            {
                outBodies.Add(Encoding.UTF8.GetString(m.Body));
            }
        }

        return outBodies;
    }

    private static List<string> PollQuiet(DefaultLitePullConsumer c, int windowMs)
    {
        var outBodies = new List<string>();
        long deadline = NowMs() + windowMs;
        while (NowMs() < deadline)
        {
            foreach (MessageExt m in c.Poll(300))
            {
                outBodies.Add(Encoding.UTF8.GetString(m.Body));
            }
        }

        return outBodies;
    }

    private sealed class CollectListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<string> _bodies = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                foreach (MessageExt m in msgs) _bodies.Add(Encoding.UTF8.GetString(m.Body));
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public bool Has(string body)
        {
            lock (_lk) return _bodies.Contains(body);
        }

        public List<string> Snapshot()
        {
            lock (_lk) return new List<string>(_bodies);
        }
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        string stamp = (NowMs() % 1000000).ToString(CultureInfo.InvariantCulture);
        string topic = "LiteCodeNet_" + stamp;
        Console.WriteLine("namesrv=" + namesrv + " topic=" + topic);

        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(namesrv);
        admin.SetTimeoutMillis(10000);
        admin.Start();

        var prod = new DefaultMQProducer("LiteCodeNet_pg_" + stamp)
        {
            NamesrvAddr = namesrv,
            InstanceName = "lite-code-net-prod-" + stamp,
        };
        var consumers = new List<DefaultLitePullConsumer>();
        DefaultMQPushConsumer? push = null;
        string brokerAddr = "";
        string original = "<none>";
        bool haveOriginal = false;
        try
        {
            ClusterInfo cluster = admin.FetchBrokerClusterInfo();
            List<string> addrs = cluster.GetBrokerAddrs();
            if (addrs.Count == 0)
            {
            Check("the cluster liveness probe", false, "the nameServer has no broker registered");
                return Report();
            }

            brokerAddr = addrs[0];
            admin.CreateTopic(MixAll.DefaultTopic, topic, 1);
            Thread.Sleep(3000);

            List<MessageQueue> mqs = admin.ExamineTopicRoute(topic).GetAllSubscribeMessageQueue(topic);
            if (mqs.Count == 0)
            {
          Check("the route is visible: 1 queue", false, "got=0");
                return Report();
            }

            MessageQueue q0 = mqs[0];
            Check("the route is visible: 1 queue", true, "queueId=" + N(q0.QueueId));
            prod.Start();

   // ---------------- R0 the switch baseline ----------------
            original = ReadFlag(admin, brokerAddr);
            haveOriginal = original != "<missing>" && original != "<error>";
     Check("R0 can read the broker's " + KConfigKey, haveOriginal, "value=" + original);
            if (original != "true")
            {
           Check("R0 " + KConfigKey + " has been temporarily turned on", WriteFlag(admin, brokerAddr, "true"));
            }

            if (ReadFlag(admin, brokerAddr) != "true")
            {
      Check("R0 the switch is not true, so the later checks are meaningless", false);
                return Report();
            }

     // ---------------- S1 the baseline ----------------
            Console.WriteLine();
       Console.WriteLine("S1 the switch is true: the lite pull baseline");
            prod.Send(new Message(topic, Encoding.UTF8.GetBytes("lite-code-s1")), q0);
            DefaultLitePullConsumer c1 = NewLite(namesrv, "GID_LiteCodeNet_g1_" + stamp, q0, stamp);
            consumers.Add(c1);
            List<string> got1 = Drain(c1, 1, 20000);
          Check("S1 the lite consumer received the message", got1.Contains("lite-code-s1"), "got=" + Join(got1));
      Check("S1 the pull cursor advanced", c1.PullCursorOf(q0) >= 1,
                "cursor=" + N(c1.PullCursorOf(q0)));

            // ---------------- S2 the switch is false ----------------
            Console.WriteLine();
       Console.WriteLine("S2 turning " + KConfigKey + " off at runtime (UPDATE_BROKER_CONFIG, no broker restart)");
            Check("S2 the switch was changed to false", WriteFlag(admin, brokerAddr, "false"));
            string readBack = ReadFlag(admin, brokerAddr);
       Check("S2 the switch reads back as confirmed", readBack == "false", "value=" + readBack);

            prod.Send(new Message(topic, Encoding.UTF8.GetBytes("lite-code-s2")), q0);

            // S2a: a bare 361 -> NO_PERMISSION + the fixed remark (going through our own client API to pick the code)
            MQClientInstance client = admin.Client();
            bool denied = false;
            string denyDetail;
            try
            {
                client.PullMessage("GID_LiteCodeNet_g1_" + stamp, q0, 0, 32, LiteFlag(), 0,
                    "*", 0, ExpressionType.TAG, 30000, -1, 15000, brokerAddr, 0, null);
        denyDetail = "it unexpectedly got SUCCESS -- the lite bit / code did not take effect";
            }
            catch (MQBrokerException e)
            {
                denied = e.ResponseCode == 16 && e.ResponseMessage.Contains(KDenyRemark);
                denyDetail = "code=" + N(e.ResponseCode) + " remark=" + e.ResponseMessage;
            }
            catch (Exception e)
            {
                denyDetail = e.Message;
            }

    Check("S2a a bare 361 is rejected by the switch (NO_PERMISSION=16)", denied, denyDetail);

   // S2b: a bare 11 on the same queue at the same offset -> messages as usual (the control group)
            bool classicOk = false;
            string classicDetail;
            try
            {
                PullResult r = client.PullMessage("GID_LiteCodeNet_g1_" + stamp, q0, 0, 32,
                    ClassicFlag(), 0, "*", 0, ExpressionType.TAG, 30000, -1, 15000,
                    brokerAddr, 0, null);
                var got11 = new List<string>();
                foreach (MessageExt m in r.MsgFoundList) got11.Add(Encoding.UTF8.GetString(m.Body));
                classicOk = got11.Count > 0;
                classicDetail = "status=" + r.Status + " got=" + Join(got11);
            }
            catch (Exception e)
            {
                classicDetail = e.Message;
            }

    Check("S2b the control group: a bare 11 is unaffected by the switch and gets messages as usual", classicOk, classicDetail);

            // S2c: the lite consumer quietly starves
            DefaultLitePullConsumer c2 = NewLite(namesrv, "GID_LiteCodeNet_g2_" + stamp, q0, stamp);
            consumers.Add(c2);
            List<string> starved = PollQuiet(c2, 8000);
            Check("S2c during the switch being off the lite consumer receives nothing at all", starved.Count == 0,
                "got=" + Join(starved));
  Check("S2c the pull cursor does not move at all", c2.PullCursorOf(q0) == 0,
                "cursor=" + N(c2.PullCursorOf(q0)));

   // S2d: a push consumer consumes as usual (the control group)
            var sink = new CollectListener();
            push = new DefaultMQPushConsumer("GID_LiteCodeNet_push_" + stamp)
            {
                InstanceName = "lite-code-net-push-" + stamp,
                ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
            };
            push.SetNamesrvAddr(namesrv);
            push.Subscribe(topic, "*");
            push.SetMessageListener(sink);
            push.Start();
            prod.Send(new Message(topic, Encoding.UTF8.GetBytes("lite-code-s2d")), q0);
            long deadline = NowMs() + 20000;
            while (!sink.Has("lite-code-s2d") && NowMs() < deadline)
            {
                Thread.Sleep(300);
            }

   Check("S2d the control group: a push consumer still receives messages (the switch only covers lite)", sink.Has("lite-code-s2d"),
                "got=" + Join(sink.Snapshot()));

            // ---------------- S3 restoring ----------------
            Console.WriteLine();
    Console.WriteLine("S3 the switch is restored to true: lite recovers");
            Check("S3 the switch has been restored", WriteFlag(admin, brokerAddr, "true"));
            prod.Send(new Message(topic, Encoding.UTF8.GetBytes("lite-code-s3")), q0);
            DefaultLitePullConsumer c3 = NewLite(namesrv, "GID_LiteCodeNet_g3_" + stamp, q0, stamp);
            consumers.Add(c3);
            List<string> got3 = Drain(c3, 1, 20000);
    Check("S3 the lite consumer recovers at once after the restore", got3.Count > 0, "got=" + Join(got3));
        }
        catch (Exception e)
        {
     Check("the scenario threw", false, e.Message);
        }
        finally
        {
            foreach (DefaultLitePullConsumer c in consumers)
            {
                try
                {
                    c.Shutdown();
                }
                catch (Exception)
                {
      // a cleanup failure does not mask the main assertion
                }
            }

            if (push is not null)
            {
                try
                {
                    push.Shutdown();
                }
                catch (Exception)
                {
                }
            }

            if (haveOriginal)
            {
                bool ok = WriteFlag(admin, brokerAddr, original);
                Console.WriteLine();
                Console.WriteLine("[restore] " + KConfigKey + "=" + original + " → "
                    + (ok ? "OK" : "FAILED"));
            }

            try
            {
                prod.Shutdown();
            }
            catch (Exception)
            {
            }

            try
            {
                admin.DeleteTopic(topic);
            }
            catch (Exception e)
            {
                Console.WriteLine("  [WARN] deleteTopic(" + topic + ") failed: " + e.Message);
            }

            admin.Shutdown();
        }

        return Report();
    }

    private static int Report()
    {
        Console.WriteLine();
        Console.WriteLine("LitePullCode live: PASS=" + _pass + " FAIL=" + _fail);
        return _fail == 0 ? 0 : 1;
    }
}
