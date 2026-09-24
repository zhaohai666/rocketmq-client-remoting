// 路由刷新周期 / 位点落盘周期 真机验证（.NET 参考 python/verify_interval_live.py 的 I1–I3）。
//
// 对应用户要求「基于真实集群测试是否正常」：`pollNameServerInterval`（Java 默认 30s）与
// `persistConsumerOffsetInterval`（Java 默认 5s）都是**以时间为唯一可观测量**的配置项，
// 不接真集群就只能断言「字段被读到了」，证明不了「周期真的按配置走」。这里两段都用真机行为断言：
//
//   I1 路由刷新周期：两个生产者同时 Start，周期分别设 1000ms 与 Java 默认 30000ms；两者都
//      用 `MQClientInstance.RegisterTopicInUse` 把一个**尚未创建**的 topic 登记进周期刷新
//      集合。之后才用 admin 建 topic（NameServer 侧路由立即可见），于是「缓存里何时出现这个
//      topic」只由各自的刷新周期决定：
//        * 1s 组 ≤6s 看到；
//        * 30s 组在那一刻**还看不到**（下一次刷新在启动后 30s）；
//        * 30s 组最终也在 ≤40s 内看到（默认值不是「卡死」，只是慢 30 倍）。
//      观测点必须用**只读缓存探测** `FindBrokerAddrByTopic`（Java `findBrokerAddrByTopic:1390`
//      就是只读缓存）：`GetTopicRouteData` 未命中会立刻拉一次，用它等于自己把缓存填上，
//      周期就不可观测了。
//
//   I2 位点落盘周期：两个消费者（周期 1000ms / 60000ms）消费同一 topic 的 3 条消息后
//      **不 commit、不 Shutdown**，broker 侧位点只能由后台周期任务推上去。断言：
//        * 首次落盘发生在 Start 后 ~10s（Java `scheduleAtFixedRate` 的 initialDelay
//          1000*10，不是立刻）；
//        * 再发 3 条：1s 组在 5s 内把位点推到 6；此刻 60s 组仍是 3（它的下一次落盘在 ~70s）；
//        * 60s 组 `Shutdown()` 时把 6 落盘（Java persistConsumerOffset 的收尾语义），
//          证明它只是「周期没到」，不是坏了。
//
//   I3 透传：生产者/消费者 Start 之后，真机实例上的 `PollNameServerIntervalMillis` 就是调用方
//      设的值（不是只在门面上存着）。
//
// 前置：NameServer + Broker 已起（本仓库 /tmp/rmq_rust_live/broker.conf）。
// 用法：rmq scheduled-intervals [namesrv]
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveScheduledIntervals
{
    private static string _namesrv = "127.0.0.1:9876";

    private static int _pass;
    private static int _fail;
    private static int _skip;

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

    private static void Skip(string name, string detail)
    {
        Interlocked.Increment(ref _skip);
        Console.WriteLine("  [SKIP] " + name + "  " + detail);
    }

    private static byte[] Str2Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>真机投递受 broker 长轮询/流控与同机负载影响，固定 sleep 会把「实现没问题」
    /// 测成假失败。</summary>
    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 200)
    {
        long deadline = NowMs() + timeoutMs;
        while (NowMs() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(intervalMs);
        }

        return pred();
    }

    private static string Secs(long fromMs, long toMs) =>
        ((toMs - fromMs) / 1000.0).ToString("F2", CultureInfo.InvariantCulture) + "s";

    /// <summary>broker 上该 group 这条队列的已提交位点（setZeroIfNotFound ⇒ 没提交过回 0，
    /// 与 Python `_broker_offset` 同口径）。判据是「broker 侧真的收到了位点」。</summary>
    private static long BrokerOffset(DefaultMQAdminExt admin, string group, MessageQueue mq)
    {
        admin.Client().QueryConsumerOffset(group, mq, out long off, 5000, null,
            setZeroIfNotFound: true);
        return off;
    }

    private sealed class CollectingListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<string> _bodies = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                foreach (MessageExt m in msgs)
                {
                    _bodies.Add(Encoding.UTF8.GetString(m.Body));
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public int Count()
        {
            lock (_lk) return _bodies.Count;
        }
    }

    public static int Run(string[] args)
    {
        if (args.Length > 0) _namesrv = args[0];
        string stamp = NowMs().ToString(CultureInfo.InvariantCulture);
        string tPoll = "IntervalPollTopic_dotnet_" + stamp;
        string tPersist = "IntervalPersistTopic_dotnet_" + stamp;
        string gPollFast = "G_poll_fast_dotnet_" + stamp;
        string gPollSlow = "G_poll_slow_dotnet_" + stamp;
        string gFast = "G_persist_fast_dotnet_" + stamp;
        string gSlow = "G_persist_slow_dotnet_" + stamp;
        string[] groups = [gPollFast, gPollSlow, gFast, gSlow];
        string[] topics = [tPoll, tPersist];
        const int fastPollMs = 1000;
        const int slowPollMs = 30000; // Java ClientConfig.pollNameServerInterval 默认值
        const int fastPersistMs = 1000;
        const int slowPersistMs = 60000;
        // Java startScheduledTask:423 的 initialDelay = 1000 * 10
        const double persistInitialDelaySec = 10.0;
        Console.WriteLine("namesrv = " + _namesrv + "  stamp = " + stamp);

        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_namesrv);
        admin.Start();
        ClusterInfo cluster;
        try
        {
            cluster = admin.FetchBrokerClusterInfo();
        }
        catch (Exception e)
        {
            Check("集群探活", false, "fetchBrokerClusterInfo: " + e.Message);
            admin.Shutdown();
            return Report();
        }

        List<string> addrs = cluster.GetBrokerAddrs();
        if (addrs.Count == 0)
        {
            Check("集群探活", false, "nameServer 无 broker 注册");
            admin.Shutdown();
            return Report();
        }

        string brokerAddr = addrs[0];
        Check("集群探活", true, "broker=" + brokerAddr);

        DefaultMQProducer? producer = null;
        var pollProducers = new List<DefaultMQProducer>();
        var consumers = new List<DefaultMQPushConsumer>();
        try
        {
            // ---------------- I1 路由刷新周期 ----------------
            var fastPoll = new DefaultMQProducer(gPollFast);
            fastPoll.NamesrvAddr = _namesrv;
            fastPoll.InstanceName = "interval-poll-fast-" + stamp;
            fastPoll.PollNameServerIntervalMillis = fastPollMs;
            fastPoll.Start();
            pollProducers.Add(fastPoll);

            var slowPoll = new DefaultMQProducer(gPollSlow);
            slowPoll.NamesrvAddr = _namesrv;
            slowPoll.InstanceName = "interval-poll-slow-" + stamp;
            slowPoll.Start(); // 不设周期 = Java 默认 30s
            pollProducers.Add(slowPoll);

            Check("I3 生产者实例拿到配置的刷新周期（1s 组）",
                fastPoll.Client().PollNameServerIntervalMillis == fastPollMs,
                "client.pollNameServerIntervalMillis="
                + fastPoll.Client().PollNameServerIntervalMillis.ToString(CultureInfo.InvariantCulture));
            Check("I3 生产者实例默认 30s（对照组）",
                slowPoll.Client().PollNameServerIntervalMillis == slowPollMs,
                "client.pollNameServerIntervalMillis="
                + slowPoll.Client().PollNameServerIntervalMillis.ToString(CultureInfo.InvariantCulture));

            // 把一个还没创建的 topic 登记进周期刷新集合：两个生产者都不会给它发消息，
            // 所以缓存里何时出现它，只由各自的刷新周期决定。
            fastPoll.Client().RegisterTopicInUse(tPoll);
            slowPoll.Client().RegisterTopicInUse(tPoll);
            Check("I1 两个生产者都把 " + tPoll + " 登记进在用 topic 集合（登记本身不拉取）",
                fastPoll.Client().FindBrokerAddrByTopic(tPoll) is null
                && slowPoll.Client().FindBrokerAddrByTopic(tPoll) is null,
                "登记后两边缓存都为空");

            // 先让两个实例各自的**首跳**（Java scheduleAtFixedRate 的 initialDelay=10ms）跑完并把
            // 这个还不存在的 topic 拉失败一次，再去建 topic。否则首跳可能落在建 topic 之后：
            // 那一跳对两组都是"第一次拉"，30s 组照样当场拿到路由，两组间隔就退化成一个传输 RTT，
            // 这条对照实验也就失去意义（真机上表现为 30s 组和 1s 组同时命中）。
            Thread.Sleep(1500);

            long t0 = NowMs();
            admin.CreateTopic(MixAll.DefaultTopic, tPoll, 1);
            Check("I1 admin 建 topic 成功", true, tPoll + " 1 队列");

            bool fastOk = WaitUntil(() => fastPoll.Client().FindBrokerAddrByTopic(tPoll) is not null,
                6000, 100);
            long dtFast = NowMs() - t0;
            Check("I1 1s 周期组 ≤6s 从 NameServer 拉到新 topic 路由", fastOk,
                "dt=" + Secs(t0, NowMs()) + " 周期=" + fastPollMs.ToString(CultureInfo.InvariantCulture) + "ms");
            Check("I1 此刻 30s 周期组**还**没拉到（对照：周期决定时机）",
                slowPoll.Client().FindBrokerAddrByTopic(tPoll) is null,
                "dt=" + Secs(t0, NowMs()) + " 周期=" + slowPollMs.ToString(CultureInfo.InvariantCulture) + "ms");

            bool slowOk = WaitUntil(() => slowPoll.Client().FindBrokerAddrByTopic(tPoll) is not null,
                40000, 500);
            long dtSlow = NowMs() - t0;
            Check("I1 30s 周期组最终也拉到（默认值只是慢，不是坏）", slowOk,
                "dt=" + Secs(t0, NowMs()) + " 周期=" + slowPollMs.ToString(CultureInfo.InvariantCulture) + "ms");
            Check("I1 两组间隔与配置同量级（30s 组至少晚 20s）", dtSlow - dtFast >= 20000,
                "fast=" + Secs(0, dtFast) + " slow=" + Secs(0, dtSlow));
            fastPoll.Shutdown();
            slowPoll.Shutdown();

            // ---------------- I2 位点落盘周期 ----------------
            admin.CreateTopic(MixAll.DefaultTopic, tPersist, 1);
            TopicRouteData persistRoute = admin.ExamineTopicRoute(tPersist);
            List<MessageQueue> persistQueues = persistRoute.GetAllMessageQueue(tPersist);
            if (persistQueues.Count == 0)
            {
                Check("I2 拿到 1 队列 topic 的队列表", false, "examineTopicRoute 没返回队列");
                throw new InvalidOperationException("no queue for " + tPersist);
            }

            MessageQueue mq = persistQueues[0];

            producer = new DefaultMQProducer("G_persist_producer_dotnet_" + stamp);
            producer.NamesrvAddr = _namesrv;
            producer.InstanceName = "interval-persist-prod-" + stamp;
            producer.Start();
            for (int i = 0; i < 3; ++i)
            {
                producer.Send(new Message(tPersist,
                    Str2Bytes("batch1-" + i.ToString(CultureInfo.InvariantCulture))));
            }

            var sinkFast = new CollectingListener();
            var sinkSlow = new CollectingListener();
            var fastC = new DefaultMQPushConsumer(gFast);
            fastC.SetNamesrvAddr(_namesrv);
            fastC.InstanceName = "interval-persist-fast-" + stamp;
            fastC.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
            fastC.PollNameServerIntervalMillis = 2000;
            fastC.PersistConsumerOffsetIntervalMillis = fastPersistMs;
            fastC.SetMessageListener(sinkFast);
            fastC.Subscribe(tPersist, "*");

            var slowC = new DefaultMQPushConsumer(gSlow);
            slowC.SetNamesrvAddr(_namesrv);
            slowC.InstanceName = "interval-persist-slow-" + stamp;
            slowC.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
            slowC.PersistConsumerOffsetIntervalMillis = slowPersistMs;
            slowC.SetMessageListener(sinkSlow);
            slowC.Subscribe(tPersist, "*");

            long tStart = NowMs();
            fastC.Start();
            consumers.Add(fastC);
            Check("I3 消费者实例拿到配置的刷新周期",
                fastC.Client().PollNameServerIntervalMillis == 2000,
                "client.pollNameServerIntervalMillis="
                + fastC.Client().PollNameServerIntervalMillis.ToString(CultureInfo.InvariantCulture));
            slowC.Start();
            consumers.Add(slowC);

            bool consumed = WaitUntil(() => sinkFast.Count() >= 3 && sinkSlow.Count() >= 3, 30000);
            Check("I2 两个消费者都消费到 3 条（尚未 commit / shutdown）", consumed,
                "fast=" + sinkFast.Count().ToString(CultureInfo.InvariantCulture)
                + " slow=" + sinkSlow.Count().ToString(CultureInfo.InvariantCulture));

            // 首笔落盘在 Start 后 ~10s：消费若在 10s 内完成，此刻两边都还没推上去。
            // 消费本身慢过 10s 时这一格不再有判别力，记为 SKIP 而不是硬报。
            if (NowMs() - tStart < 9000)
            {
                long offFastNow = BrokerOffset(admin, gFast, mq);
                long offSlowNow = BrokerOffset(admin, gSlow, mq);
                Check("I2 消费后 broker 位点还没有立刻被推上去", offFastNow < 3 && offSlowNow < 3,
                    "fast=" + offFastNow.ToString(CultureInfo.InvariantCulture)
                    + " slow=" + offSlowNow.ToString(CultureInfo.InvariantCulture)
                    + " elapsed=" + Secs(tStart, NowMs()));
            }
            else
            {
                Skip("I2 消费后 broker 位点还没有立刻被推上去",
                    "消费耗时 " + Secs(tStart, NowMs()) + " ≥ 10s，已越过首笔落盘窗口");
            }

            bool fastFirst = WaitUntil(() => BrokerOffset(admin, gFast, mq) == 3, 20000, 300);
            long dtFastFirst = NowMs() - tStart;
            bool slowFirst = WaitUntil(() => BrokerOffset(admin, gSlow, mq) == 3, 20000, 300);
            long dtSlowFirst = NowMs() - tStart;
            Check("I2 1s 组首次落盘发生在 initialDelay(~10s) 之后", fastFirst,
                "dt=" + Secs(0, dtFastFirst) + " 周期=" + fastPersistMs.ToString(CultureInfo.InvariantCulture) + "ms");
            Check("I2 首笔落盘不早于 Java 的 initialDelay 10s",
                fastFirst && dtFastFirst / 1000.0 >= persistInitialDelaySec - 0.5,
                "dt=" + Secs(0, dtFastFirst));
            Check("I2 60s 组同样在 ~10s 完成首笔落盘（周期未到，先走 initialDelay）", slowFirst,
                "dt=" + Secs(0, dtSlowFirst) + " 周期=" + slowPersistMs.ToString(CultureInfo.InvariantCulture) + "ms");

            // 第二批：两组都会消费到，但 broker 侧位点只由各自的周期任务推上去。
            long t2 = NowMs();
            for (int i = 0; i < 3; ++i)
            {
                producer.Send(new Message(tPersist,
                    Str2Bytes("batch2-" + i.ToString(CultureInfo.InvariantCulture))));
            }

            Check("I2 两个消费者都消费到第二批（6 条）",
                WaitUntil(() => sinkFast.Count() >= 6 && sinkSlow.Count() >= 6, 20000),
                "fast=" + sinkFast.Count().ToString(CultureInfo.InvariantCulture)
                + " slow=" + sinkSlow.Count().ToString(CultureInfo.InvariantCulture));
            bool fastSecond = WaitUntil(() => BrokerOffset(admin, gFast, mq) == 6, 5000, 200);
            Check("I2 1s 组一个周期内把第二批位点推上去", fastSecond,
                "dt=" + Secs(t2, NowMs()) + " 周期=" + fastPersistMs.ToString(CultureInfo.InvariantCulture) + "ms");
            long offSlowBatch2 = BrokerOffset(admin, gSlow, mq);
            Check("I2 此刻 60s 组仍是 3（周期 60s 远未到，且它确实消费到了 6）",
                offSlowBatch2 == 3 && sinkSlow.Count() >= 6,
                "broker=" + offSlowBatch2.ToString(CultureInfo.InvariantCulture)
                + " 已消费=" + sinkSlow.Count().ToString(CultureInfo.InvariantCulture)
                + " 周期=" + slowPersistMs.ToString(CultureInfo.InvariantCulture) + "ms");
            long sinceSlowFirst = NowMs() - (tStart + dtSlowFirst);
            Check("I2 60s 组的下一次周期还没到（距首笔落盘 < 周期 60s）",
                sinceSlowFirst < slowPersistMs, "elapsed=" + Secs(0, sinceSlowFirst));

            slowC.Shutdown(); // Java persistConsumerOffset 的收尾语义：退出前把内存位点落盘
            consumers.Remove(slowC);
            bool slowSecond = WaitUntil(() => BrokerOffset(admin, gSlow, mq) == 6, 10000, 300);
            Check("I2 60s 组 Shutdown() 时把 6 落盘（Java persistConsumerOffset 收尾）", slowSecond,
                "broker=" + BrokerOffset(admin, gSlow, mq).ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e)
        {
            Check("验证过程抛出异常", false, e.ToString());
        }

        // ---------------- 清理 ----------------
        foreach (DefaultMQPushConsumer c in consumers)
        {
            try
            {
                c.Shutdown();
            }
            catch (Exception e)
            {
                Console.WriteLine("    (consumer shutdown 失败: " + e.Message + ")");
            }
        }

        if (producer is not null)
        {
            try
            {
                producer.Shutdown();
            }
            catch (Exception e)
            {
                Console.WriteLine("    (producer shutdown 失败: " + e.Message + ")");
            }
        }

        foreach (DefaultMQProducer p in pollProducers)
        {
            try
            {
                p.Shutdown();
            }
            catch (Exception e)
            {
                Console.WriteLine("    (producer shutdown 失败: " + e.Message + ")");
            }
        }

        foreach (string t in topics)
        {
            try
            {
                admin.DeleteTopic(t);
                Console.WriteLine("    (deleteTopic(" + t + ") OK)");
            }
            catch (Exception e)
            {
                Console.WriteLine("    (deleteTopic(" + t + ") 失败: " + e.Message + ")");
            }
        }

        foreach (string g in groups)
        {
            try
            {
                admin.DeleteSubscriptionGroup(brokerAddr, g, removeOffset: true);
            }
            catch (Exception e)
            {
                Console.WriteLine("    (deleteSubscriptionGroup(" + g + ") 失败: " + e.Message + ")");
            }
        }

        admin.Shutdown();
        return Report();
    }

    private static int Report()
    {
        Console.WriteLine();
        Console.WriteLine("PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
                          + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture)
                          + " SKIP=" + _skip.ToString(CultureInfo.InvariantCulture));
        return _fail == 0 ? 0 : 1;
    }
}
