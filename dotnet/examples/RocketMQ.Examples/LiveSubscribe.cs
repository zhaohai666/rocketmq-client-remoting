// 后置订阅真机验证（.NET 对齐 Java `subscribe` 之后的「立即推一轮心跳」，与
// python/verify_subscribe_live.py、rust/examples/live_subscribe.rs、
// cpp/examples/live_subscribe.cpp 一一对应）。
//
// Java `DefaultMQPushConsumerImpl.subscribe:1265-1275` 只做两件事：
// `subscriptionInner.put(...)` + `if (this.mQClientFactory != null)
// this.mQClientFactory.sendHeartbeatToAllBrokerWithLock();` —— 允许 `Start()` 之后订阅，
// 而且**同步**推一轮心跳。观测点是 broker 的 topic→group 表
// （`ConsumerManager#registerConsumer` 维护，`QUERY_TOPIC_CONSUME_BY_WHO(300)` 读取）：
// 订阅路径不推心跳的话，表里要等下一个 30s 心跳周期才出现本组。
//
//   S0 正对照：`Start()` 之后基础 topic B 已登记本组（心跳链路与 300 查询本身是通的）。
//   S1 负对照：本轮**还没**订阅的 L，300 查不到本组。
//   S2 后置订阅立即生效：`Subscribe(L)` 之后不睡直接查 300(L) → 本组已在表里，
//      且耗时远小于心跳周期（默认 30s）⇒ 只可能来自订阅路径那一轮同步心跳。
//   S3 后置订阅真会被消费：L 进分配集 → 发一条消息 → listener 收到。
//   S4 活订阅表：`Unsubscribe(L)` 后本组订阅集立刻少掉 L。
//      （Java:1317-1319 只删表项、**不**推心跳，且 broker 的 topicGroupTable 只在整组
//      无订阅时才清 —— 所以这里不拿 broker 的表当断言。）
//
// 前置：NameServer + Broker 已起（本仓库 /tmp/rmq_rust_live/broker.conf）。
// 用法：rmq subscribe [namesrv]
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveSubscribe
{
    private static string _namesrv = "127.0.0.1:9876";

    private static int _pass;
    private static int _fail;

    /// <summary>Java ClientConfig#heartbeatBrokerInterval 默认 30s：登记必须远快于它。</summary>
    private const long HeartbeatPeriodMs = 30000;

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

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>真机投递受 broker 长轮询/流控与同机负载影响，固定 sleep 会把「实现没问题」
    /// 测成假失败。</summary>
    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 100)
    {
        long deadline = NowMs() + timeoutMs;
        while (NowMs() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(intervalMs);
        }

        return pred();
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

        public List<string> Snapshot()
        {
            lock (_lk) return new List<string>(_bodies);
        }
    }

    private static string Join(IEnumerable<string> v) =>
        "[" + string.Join(", ", v) + "]";

    public static int Run(string[] args)
    {
        if (args.Length > 0) _namesrv = args[0];
        string stamp = NowMs().ToString(CultureInfo.InvariantCulture);
        string tBase = "SubBaseTopic_dotnet_" + stamp;
        string tLate = "SubLateTopic_dotnet_" + stamp;
        string group = "G_sub_after_start_dotnet_" + stamp;
        string pGroup = "G_sub_producer_dotnet_" + stamp;
        Console.WriteLine("namesrv = " + _namesrv + "  stamp = " + stamp);

        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_namesrv);
        admin.Start();
        string brokerAddr;
        try
        {
            ClusterInfo cluster = admin.FetchBrokerClusterInfo();
            List<string> addrs = cluster.GetBrokerAddrs();
            if (addrs.Count == 0)
            {
                Check("集群探活", false, "nameServer 无 broker 注册");
                admin.Shutdown();
                return Report();
            }

            brokerAddr = addrs[0];
        }
        catch (Exception e)
        {
            Check("集群探活", false, "fetchBrokerClusterInfo: " + e.Message);
            admin.Shutdown();
            return Report();
        }

        Check("集群探活", true, "broker=" + brokerAddr);

        DefaultMQPushConsumer? consumer = null;
        DefaultMQProducer? producer = null;
        var sink = new CollectingListener();
        try
        {
            // 先建 topic：消费者不做默认 topic 兜底（对齐 Java），topic 不存在就拿不到路由。
            admin.CreateTopic(MixAll.DefaultTopic, tBase, 4);
            admin.CreateTopic(MixAll.DefaultTopic, tLate, 4);

            consumer = new DefaultMQPushConsumer(group)
            {
                InstanceName = "live-subscribe-" + stamp,
            };
            consumer.SetNamesrvAddr(_namesrv);
            consumer.Subscribe(tBase, "*");
            consumer.SetMessageListener(sink);
            consumer.Start();

            HashSet<string> Who(string topic) => admin.QueryTopicConsumeByWho(brokerAddr, topic);

            // ---------------- S0 正对照 ----------------
            long t0 = NowMs();
            bool s0 = WaitUntil(() => Who(tBase).Contains(group), 35000);
            Check("S0-基础 topic B 已登记本组（300 查得到）", s0,
                "groupList=" + Join(Who(tBase).OrderBy(x => x, StringComparer.Ordinal))
                + " elapsed=" + (NowMs() - t0).ToString(CultureInfo.InvariantCulture) + "ms");

            // ---------------- S1 负对照 ----------------
            HashSet<string> gotL0 = Who(tLate);
            Check("S1-负对照：未订阅的 L 查不到本组", !gotL0.Contains(group),
                "groupList=" + Join(gotL0.OrderBy(x => x, StringComparer.Ordinal)));

            // ---------------- S2 后置订阅立即生效 ----------------
            long t1 = NowMs();
            consumer.Subscribe(tLate, "*");
            long subscribeMs = NowMs() - t1;
            HashSet<string> gotL1 = Who(tLate);
            long elapsedMs = NowMs() - t1;
            bool registered = gotL1.Contains(group);
            Check("S2-后置订阅后 broker 立刻登记本组（300 查得到）", registered,
                "groupList=" + Join(gotL1.OrderBy(x => x, StringComparer.Ordinal)));
            // 心跳周期默认 30s：只有订阅路径那一轮**同步**心跳才能让登记这么快出现。
            Check("S2-登记耗时远小于 30s 心跳周期（只可能是订阅路径推的）",
                registered && elapsedMs < HeartbeatPeriodMs / 6,
                "subscribe 返回耗时=" + subscribeMs.ToString(CultureInfo.InvariantCulture)
                + "ms，查询完成耗时=" + elapsedMs.ToString(CultureInfo.InvariantCulture) + "ms");

            // ---------------- S3 后置订阅真会被消费 ----------------
            long t2 = NowMs();
            bool assigned = WaitUntil(
                () => consumer.AssignedQueueKeys().Any(k => k.StartsWith(tLate, StringComparison.Ordinal)),
                45000);
            List<string> lateKeys = consumer.AssignedQueueKeys()
                .Where(k => k.StartsWith(tLate, StringComparison.Ordinal)).ToList();
            Check("S3-新 topic L 进入本实例分配集（rebalance 生效）", assigned,
                "assigned=" + Join(lateKeys) + " elapsed="
                + (NowMs() - t2).ToString(CultureInfo.InvariantCulture) + "ms");

            producer = new DefaultMQProducer(pGroup)
            {
                NamesrvAddr = _namesrv,
                InstanceName = "sub-producer-" + stamp,
                SendMsgTimeout = 20000,
            };
            producer.Start();
            Thread.Sleep(500);
            producer.Send(new Message(tLate, Encoding.UTF8.GetBytes("late-subscribe-me")), 20000);
            bool consumed = WaitUntil(
                () => sink.Snapshot().Any(b => b == "late-subscribe-me"), 30000);
            Check("S3-后置订阅的 topic 上的消息真的被消费", consumed,
                "seen=" + Join(sink.Snapshot()));

            // ---------------- S4 活订阅表 ----------------
            consumer.Unsubscribe(tLate);
            List<string> live = consumer.SubscribedTopics();
            Check("S4-Unsubscribe 后本组订阅集立刻少掉 L（只删表项，不发心跳）",
                !live.Contains(tLate) && live.Contains(tBase), "live=" + Join(live));
        }
        catch (Exception e)
        {
            Check("场景执行", false, e.Message);
        }

        try { consumer?.Shutdown(); } catch (Exception e) { Console.WriteLine("    (consumer shutdown 失败: " + e.Message + ")"); }
        try { producer?.Shutdown(); } catch (Exception e) { Console.WriteLine("    (producer shutdown 失败: " + e.Message + ")"); }
        foreach (string t in new[] { tBase, tLate })
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

        try { admin.DeleteSubscriptionGroup(brokerAddr, group, true); }
        catch (Exception e) { Console.WriteLine("    (deleteSubscriptionGroup(" + group + ") 失败: " + e.Message + ")"); }
        try { admin.Shutdown(); } catch (Exception e) { Console.WriteLine("    (admin shutdown 失败: " + e.Message + ")"); }

        return Report();
    }

    private static int Report()
    {
        Console.WriteLine();
        Console.WriteLine("PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
                          + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture));
        return _fail == 0 ? 0 : 1;
    }
}
