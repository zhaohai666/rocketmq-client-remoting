// Prerequisite: a NameServer + Broker are already up (/tmp/rmq_rust_live/broker.conf in this repo).
// Usage: rmq subscribe [namesrv]
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

    /// <summary>Real-machine delivery is affected by the broker's long polling / flow
    /// control and by load on the same machine, so a fixed sleep turns "the implementation
    /// is fine" into a false failure.</summary>
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
                Check("the cluster liveness probe", false, "the nameServer has no broker registered");
                admin.Shutdown();
                return Report();
            }

            brokerAddr = addrs[0];
        }
        catch (Exception e)
        {
            Check("the cluster liveness probe", false, "fetchBrokerClusterInfo: " + e.Message);
            admin.Shutdown();
            return Report();
        }

        Check("the cluster liveness probe", true, "broker=" + brokerAddr);

        DefaultMQPushConsumer? consumer = null;
        DefaultMQProducer? producer = null;
        var sink = new CollectingListener();
        try
        {
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

            // ---------------- S0 positive control ----------------
            long t0 = NowMs();
            bool s0 = WaitUntil(() => Who(tBase).Contains(group), 35000);
            Check("S0-the base topic B has this group registered (visible to a 300 query)", s0,
                "groupList=" + Join(Who(tBase).OrderBy(x => x, StringComparer.Ordinal))
                + " elapsed=" + (NowMs() - t0).ToString(CultureInfo.InvariantCulture) + "ms");

            // ---------------- S1 negative control ----------------
            HashSet<string> gotL0 = Who(tLate);
            Check("S1-negative control: the unsubscribed L does not show this group", !gotL0.Contains(group),
                "groupList=" + Join(gotL0.OrderBy(x => x, StringComparer.Ordinal)));

            // ---------------- S2 a later subscription takes effect immediately ----------------
            long t1 = NowMs();
            consumer.Subscribe(tLate, "*");
            long subscribeMs = NowMs() - t1;
            HashSet<string> gotL1 = Who(tLate);
            long elapsedMs = NowMs() - t1;
            bool registered = gotL1.Contains(group);
            Check("S2-after the later subscription the broker registers this group at once (visible to a 300 query)", registered,
                "groupList=" + Join(gotL1.OrderBy(x => x, StringComparer.Ordinal)));
            // The heartbeat interval defaults to 30s: only the **synchronous** heartbeat on
            // the subscription path can make the registration appear this fast.
            Check("S2-registration took far less than the 30s heartbeat interval (it can only be the subscription path)",
                registered && elapsedMs < HeartbeatPeriodMs / 6,
                "subscribe returned in " + subscribeMs.ToString(CultureInfo.InvariantCulture)
                + "ms, the query completed in " + elapsedMs.ToString(CultureInfo.InvariantCulture) + "ms");

            // ---------------- S3 a later subscription really gets consumed ----------------
            long t2 = NowMs();
            bool assigned = WaitUntil(
                () => consumer.AssignedQueueKeys().Any(k => k.StartsWith(tLate, StringComparison.Ordinal)),
                45000);
            List<string> lateKeys = consumer.AssignedQueueKeys()
                .Where(k => k.StartsWith(tLate, StringComparison.Ordinal)).ToList();
            Check("S3-the new topic L enters this instance's assignment set (the rebalance took effect)", assigned,
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
            Check("S3-messages on the later-subscribed topic really are consumed", consumed,
                "seen=" + Join(sink.Snapshot()));

            // ---------------- S4 the live subscription table ----------------
            consumer.Unsubscribe(tLate);
            List<string> live = consumer.SubscribedTopics();
            Check("S4-after Unsubscribe this group's subscription set immediately loses L (only the entry is removed, no heartbeat is sent)",
                !live.Contains(tLate) && live.Contains(tBase), "live=" + Join(live));
        }
        catch (Exception e)
        {
            Check("running the scenario", false, e.Message);
        }

        try { consumer?.Shutdown(); } catch (Exception e) { Console.WriteLine("    (consumer shutdown failed: " + e.Message + ")"); }
        try { producer?.Shutdown(); } catch (Exception e) { Console.WriteLine("    (producer shutdown failed: " + e.Message + ")"); }
        foreach (string t in new[] { tBase, tLate })
        {
            try
            {
                admin.DeleteTopic(t);
                Console.WriteLine("    (deleteTopic(" + t + ") OK)");
            }
            catch (Exception e)
            {
                Console.WriteLine("    (deleteTopic(" + t + ") failed: " + e.Message + ")");
            }
        }

        try { admin.DeleteSubscriptionGroup(brokerAddr, group, true); }
        catch (Exception e) { Console.WriteLine("    (deleteSubscriptionGroup(" + group + ") failed: " + e.Message + ")"); }
        try { admin.Shutdown(); } catch (Exception e) { Console.WriteLine("    (admin shutdown failed: " + e.Message + ")"); }

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
