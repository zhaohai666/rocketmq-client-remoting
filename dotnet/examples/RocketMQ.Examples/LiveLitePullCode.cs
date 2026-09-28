// lite-pull **请求码 / broker 开关**（#107）真机验证。
// 与 python/verify_lite_pull_code_live.py、cpp/examples/live_lite_pull_code.cpp、
// rust/examples/live_lite_pull_code.rs 同题、逐条对应。
//
// 为什么必须真机：`FLAG_LITE_PULL_MESSAGE(0x10)` + `LITE_PULL_MESSAGE(361)` 这条链在离线
// 假 broker 上永远是绿的 —— 少了位、码还是 11 时，报文依然是一个完全合法的 pull，
// 假 broker（和真 broker 的普通 pull 分支）照常回消息。能把它区分出来的只有真 broker 的
// `litePullMessageEnable` 开关（`PullMessageProcessor:325-331` **只拦 361**）：把开关在
// 运行时翻成 false（UPDATE_BROKER_CONFIG，无需重启）：
//
//   S1 开关默认 true：lite pull 全链路正常（基线）。
//   S2 开关 false：
//      S2a 裸 361 请求 → NO_PERMISSION(16) + "…for lite pull consumer is forbidden"；
//      S2b 同队列同一位点的裸 11 请求 → 照常 SUCCESS 且拿到消息（**对照**：开关只管
//          lite，普通 pull 不受影响 —— 没有这条腿，S2a 的失败可能只是 broker 坏了）；
//      S2c lite 消费者安静饿死：消息明明在，poll 一条不来、拉取游标纹丝不动
//          （旧实现位不置/码为 11，这条腿会收到消息 → 判别器变红）；
//      S2d push 消费者照常消费（**对照**：整条消费链路没坏）。
//   S3 开关还原 true：lite pull 立即恢复。
//
// 退出前**无条件**把 `litePullMessageEnable` 改回原值（与 reset-offset 同款）。
//
// 用法：rmq lite-pull-code [namesrv]
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

    /// <summary>与 Java DefaultLitePullConsumerImpl#pullSyncImpl:1058 的 buildSysFlag(false, block, true, false, litePull=true) 对齐。</summary>
    private static int LiteFlag() => PullSysFlag.BuildSysFlag(commitOffset: false, suspend: false,
        subscription: true, classFilter: false, litePull: true);

    /// <summary>DefaultMQPullConsumerImpl.pullSyncImpl:248 的 4 参版本，lite 位必须为 0。</summary>
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
        // 位点由用例自己看：关掉自动提交，S2c 的「游标纹丝不动」才干净。
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
                Check("集群探活", false, "nameServer 上无 broker 注册");
                return Report();
            }

            brokerAddr = addrs[0];
            admin.CreateTopic(MixAll.DefaultTopic, topic, 1);
            Thread.Sleep(3000);

            List<MessageQueue> mqs = admin.ExamineTopicRoute(topic).GetAllSubscribeMessageQueue(topic);
            if (mqs.Count == 0)
            {
                Check("路由可见：1 条队列", false, "got=0");
                return Report();
            }

            MessageQueue q0 = mqs[0];
            Check("路由可见：1 条队列", true, "queueId=" + N(q0.QueueId));
            prod.Start();

            // ---------------- R0 开关基线 ----------------
            original = ReadFlag(admin, brokerAddr);
            haveOriginal = original != "<missing>" && original != "<error>";
            Check("R0 能读到 broker 的 " + KConfigKey, haveOriginal, "value=" + original);
            if (original != "true")
            {
                Check("R0 已临时打开 " + KConfigKey, WriteFlag(admin, brokerAddr, "true"));
            }

            if (ReadFlag(admin, brokerAddr) != "true")
            {
                Check("R0 开关不在 true，后续检查无意义", false);
                return Report();
            }

            // ---------------- S1 基线 ----------------
            Console.WriteLine();
            Console.WriteLine("S1 开关 true：lite pull 基线");
            prod.Send(new Message(topic, Encoding.UTF8.GetBytes("lite-code-s1")), q0);
            DefaultLitePullConsumer c1 = NewLite(namesrv, "GID_LiteCodeNet_g1_" + stamp, q0, stamp);
            consumers.Add(c1);
            List<string> got1 = Drain(c1, 1, 20000);
            Check("S1 lite 消费者收到消息", got1.Contains("lite-code-s1"), "got=" + Join(got1));
            Check("S1 拉取游标已推进", c1.PullCursorOf(q0) >= 1,
                "cursor=" + N(c1.PullCursorOf(q0)));

            // ---------------- S2 开关 false ----------------
            Console.WriteLine();
            Console.WriteLine("S2 运行时关闭 " + KConfigKey + "（UPDATE_BROKER_CONFIG，不重启 broker）");
            Check("S2 开关已改为 false", WriteFlag(admin, brokerAddr, "false"));
            string readBack = ReadFlag(admin, brokerAddr);
            Check("S2 开关读回确认", readBack == "false", "value=" + readBack);

            prod.Send(new Message(topic, Encoding.UTF8.GetBytes("lite-code-s2")), q0);

            // S2a：裸 361 → NO_PERMISSION + 固定 remark（走我们自己的客户端 API 选码）
            MQClientInstance client = admin.Client();
            bool denied = false;
            string denyDetail;
            try
            {
                client.PullMessage("GID_LiteCodeNet_g1_" + stamp, q0, 0, 32, LiteFlag(), 0,
                    "*", 0, ExpressionType.TAG, 30000, -1, 15000, brokerAddr, 0, null);
                denyDetail = "竟然 SUCCESS —— lite 位/码没生效";
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

            Check("S2a 裸 361 被开关拒绝（NO_PERMISSION=16）", denied, denyDetail);

            // S2b：同队列同一位点的裸 11 → 照常拿消息（对照组）
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

            Check("S2b 对照组：裸 11 不受开关影响，照常拿消息", classicOk, classicDetail);

            // S2c：lite 消费者安静饿死
            DefaultLitePullConsumer c2 = NewLite(namesrv, "GID_LiteCodeNet_g2_" + stamp, q0, stamp);
            consumers.Add(c2);
            List<string> starved = PollQuiet(c2, 8000);
            Check("S2c 开关关闭期间 lite 消费者一条都收不到", starved.Count == 0,
                "got=" + Join(starved));
            Check("S2c 拉取游标纹丝不动", c2.PullCursorOf(q0) == 0,
                "cursor=" + N(c2.PullCursorOf(q0)));

            // S2d：push 消费者照常消费（对照组）
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

            Check("S2d 对照组：push 消费者照常收到消息（开关只管 lite）", sink.Has("lite-code-s2d"),
                "got=" + Join(sink.Snapshot()));

            // ---------------- S3 还原 ----------------
            Console.WriteLine();
            Console.WriteLine("S3 开关还原 true：lite 恢复");
            Check("S3 开关已还原", WriteFlag(admin, brokerAddr, "true"));
            prod.Send(new Message(topic, Encoding.UTF8.GetBytes("lite-code-s3")), q0);
            DefaultLitePullConsumer c3 = NewLite(namesrv, "GID_LiteCodeNet_g3_" + stamp, q0, stamp);
            consumers.Add(c3);
            List<string> got3 = Drain(c3, 1, 20000);
            Check("S3 还原后 lite 消费者立即恢复", got3.Count > 0, "got=" + Join(got3));
        }
        catch (Exception e)
        {
            Check("场景异常", false, e.Message);
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
                    // 收尾失败不掩盖主断言
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
