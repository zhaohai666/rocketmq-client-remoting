// Real-machine verification of the route refresh interval / the offset flush interval
// (case ids I1-I3).
//
//
//
//
//   I3 pass-through: after a producer/consumer Start, `PollNameServerIntervalMillis` on the
//   real instance is the value the caller set (not merely stored on the facade).
//
// Prerequisite: a NameServer + Broker are up (/tmp/rmq_rust_live/broker.conf in this repo).
// Usage: rmq scheduled-intervals [namesrv]
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

   /// <summary>Real-machine delivery is affected by the broker's long polling / flow
    /// control and by load on the same machine, so a fixed sleep turns "the implementation is
    /// fine" into a false failure.</summary>
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

   /// <summary>The committed offset of that queue for that group on the broker
   /// (setZeroIfNotFound => 0 when it was never committed). The criterion is "the broker side
    /// really received the offset".</summary>
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
        const int slowPollMs = 30000;
        const int fastPersistMs = 1000;
        const int slowPersistMs = 60000;
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
  Check("the cluster liveness probe", false, "fetchBrokerClusterInfo: " + e.Message);
            admin.Shutdown();
            return Report();
        }

        List<string> addrs = cluster.GetBrokerAddrs();
        if (addrs.Count == 0)
        {
        Check("the cluster liveness probe", false, "the nameServer has no broker registered");
            admin.Shutdown();
            return Report();
        }

        string brokerAddr = addrs[0];
   Check("the cluster liveness probe", true, "broker=" + brokerAddr);

        DefaultMQProducer? producer = null;
        var pollProducers = new List<DefaultMQProducer>();
        var consumers = new List<DefaultMQPushConsumer>();
        try
        {
      // ---------------- I1 the route refresh interval ----------------
            var fastPoll = new DefaultMQProducer(gPollFast);
            fastPoll.NamesrvAddr = _namesrv;
            fastPoll.InstanceName = "interval-poll-fast-" + stamp;
            fastPoll.PollNameServerIntervalMillis = fastPollMs;
            fastPoll.Start();
            pollProducers.Add(fastPoll);

            var slowPoll = new DefaultMQProducer(gPollSlow);
            slowPoll.NamesrvAddr = _namesrv;
            slowPoll.InstanceName = "interval-poll-slow-" + stamp;
            slowPoll.Start();
            pollProducers.Add(slowPoll);

    Check("I3 the producer instance got the configured refresh interval (the 1s group)",
                fastPoll.Client().PollNameServerIntervalMillis == fastPollMs,
                "client.pollNameServerIntervalMillis="
                + fastPoll.Client().PollNameServerIntervalMillis.ToString(CultureInfo.InvariantCulture));
      Check("I3 the producer instance defaults to 30s (the control group)",
                slowPoll.Client().PollNameServerIntervalMillis == slowPollMs,
                "client.pollNameServerIntervalMillis="
                + slowPoll.Client().PollNameServerIntervalMillis.ToString(CultureInfo.InvariantCulture));

  // Register a not-yet-created topic into the periodic refresh set: neither producer sends it
  // a message, so when it appears in the cache is decided only by their own refresh intervals.
            fastPoll.Client().RegisterTopicInUse(tPoll);
            slowPoll.Client().RegisterTopicInUse(tPoll);
   Check("I1 both producers registered " + tPoll + " into the in-use topic set (registering itself does not fetch)",
                fastPoll.Client().FindBrokerAddrByTopic(tPoll) is null
                && slowPoll.Client().FindBrokerAddrByTopic(tPoll) is null,
   "both caches are empty after registering");

            Thread.Sleep(1500);

            long t0 = NowMs();
            admin.CreateTopic(MixAll.DefaultTopic, tPoll, 1);
      Check("I1 admin created the topic successfully", true, tPoll + " 1 queue");

            bool fastOk = WaitUntil(() => fastPoll.Client().FindBrokerAddrByTopic(tPoll) is not null,
                6000, 100);
            long dtFast = NowMs() - t0;
      Check("I1 the 1s group pulled the new topic's route from the NameServer within 6s", fastOk,
     "dt=" + Secs(t0, NowMs()) + " interval=" + fastPollMs.ToString(CultureInfo.InvariantCulture) + "ms");
      Check("I1 the 30s group has **not** pulled it yet at this point (the control: the interval decides the timing)",
                slowPoll.Client().FindBrokerAddrByTopic(tPoll) is null,
       "dt=" + Secs(t0, NowMs()) + " interval=" + slowPollMs.ToString(CultureInfo.InvariantCulture) + "ms");

            bool slowOk = WaitUntil(() => slowPoll.Client().FindBrokerAddrByTopic(tPoll) is not null,
                40000, 500);
            long dtSlow = NowMs() - t0;
     Check("I1 the 30s group eventually pulls it too (the default is only slower, not broken)", slowOk,
            "dt=" + Secs(t0, NowMs()) + " interval=" + slowPollMs.ToString(CultureInfo.InvariantCulture) + "ms");
            Check("I1 the two groups are an order of magnitude apart (the 30s group is at least 20s later)", dtSlow - dtFast >= 20000,
                "fast=" + Secs(0, dtFast) + " slow=" + Secs(0, dtSlow));
            fastPoll.Shutdown();
            slowPoll.Shutdown();

      // ---------------- I2 the offset flush interval ----------------
            admin.CreateTopic(MixAll.DefaultTopic, tPersist, 1);
            TopicRouteData persistRoute = admin.ExamineTopicRoute(tPersist);
            List<MessageQueue> persistQueues = persistRoute.GetAllMessageQueue(tPersist);
            if (persistQueues.Count == 0)
            {
      Check("I2 got the queue list of the 1-queue topic", false, "examineTopicRoute returned no queues");
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
        Check("I3 the consumer instance got the configured refresh interval",
                fastC.Client().PollNameServerIntervalMillis == 2000,
                "client.pollNameServerIntervalMillis="
                + fastC.Client().PollNameServerIntervalMillis.ToString(CultureInfo.InvariantCulture));
            slowC.Start();
            consumers.Add(slowC);

            bool consumed = WaitUntil(() => sinkFast.Count() >= 3 && sinkSlow.Count() >= 3, 30000);
          Check("I2 both consumers consumed 3 (not yet committed / shut down)", consumed,
                "fast=" + sinkFast.Count().ToString(CultureInfo.InvariantCulture)
                + " slow=" + sinkSlow.Count().ToString(CultureInfo.InvariantCulture));

            // The first flush lands about 10s after Start: if the consumption finished within
            // 10s, neither side has pushed anything up at this point. When the consumption
  // itself takes longer than 10s this slot loses its discriminating power, so it is
       // recorded as SKIP rather than reported as a hard failure.
            if (NowMs() - tStart < 9000)
            {
                long offFastNow = BrokerOffset(admin, gFast, mq);
                long offSlowNow = BrokerOffset(admin, gSlow, mq);
        Check("I2 after the consumption the broker offset was not pushed up immediately", offFastNow < 3 && offSlowNow < 3,
                    "fast=" + offFastNow.ToString(CultureInfo.InvariantCulture)
                    + " slow=" + offSlowNow.ToString(CultureInfo.InvariantCulture)
                    + " elapsed=" + Secs(tStart, NowMs()));
            }
            else
            {
       Skip("I2 after the consumption the broker offset was not pushed up immediately",
  "the consumption took " + Secs(tStart, NowMs()) + " >= 10s, it has passed the first flush window");
            }

            bool fastFirst = WaitUntil(() => BrokerOffset(admin, gFast, mq) == 3, 20000, 300);
            long dtFastFirst = NowMs() - tStart;
            bool slowFirst = WaitUntil(() => BrokerOffset(admin, gSlow, mq) == 3, 20000, 300);
            long dtSlowFirst = NowMs() - tStart;
        Check("I2 the 1s group's first flush happens after the initialDelay (~10s)", fastFirst,
      "dt=" + Secs(0, dtFastFirst) + " interval=" + fastPersistMs.ToString(CultureInfo.InvariantCulture) + "ms");
  Check("I2 the first flush is not earlier than the initialDelay of 10s",
                fastFirst && dtFastFirst / 1000.0 >= persistInitialDelaySec - 0.5,
                "dt=" + Secs(0, dtFastFirst));
   Check("I2 the 60s group also completes its first flush at ~10s (the interval has not arrived, so the initialDelay comes first)", slowFirst,
       "dt=" + Secs(0, dtSlowFirst) + " interval=" + slowPersistMs.ToString(CultureInfo.InvariantCulture) + "ms");

            // Second batch: both groups consume it, but the broker-side offset is only pushed
     // up by their own periodic tasks.
            long t2 = NowMs();
            for (int i = 0; i < 3; ++i)
            {
                producer.Send(new Message(tPersist,
                    Str2Bytes("batch2-" + i.ToString(CultureInfo.InvariantCulture))));
            }

       Check("I2 both consumers consumed the second batch (6 in total)",
                WaitUntil(() => sinkFast.Count() >= 6 && sinkSlow.Count() >= 6, 20000),
                "fast=" + sinkFast.Count().ToString(CultureInfo.InvariantCulture)
                + " slow=" + sinkSlow.Count().ToString(CultureInfo.InvariantCulture));
            bool fastSecond = WaitUntil(() => BrokerOffset(admin, gFast, mq) == 6, 5000, 200);
  Check("I2 the 1s group pushes the second batch's offset up within one interval", fastSecond,
     "dt=" + Secs(t2, NowMs()) + " interval=" + fastPersistMs.ToString(CultureInfo.InvariantCulture) + "ms");
            long offSlowBatch2 = BrokerOffset(admin, gSlow, mq);
    Check("I2 the 60s group is still at 3 at this point (the 60s interval is far from due and it really consumed 6)",
                offSlowBatch2 == 3 && sinkSlow.Count() >= 6,
                "broker=" + offSlowBatch2.ToString(CultureInfo.InvariantCulture)
     + " consumed=" + sinkSlow.Count().ToString(CultureInfo.InvariantCulture)
             + " interval=" + slowPersistMs.ToString(CultureInfo.InvariantCulture) + "ms");
            long sinceSlowFirst = NowMs() - (tStart + dtSlowFirst);
      Check("I2 the 60s group's next interval has not arrived yet (less than the 60s interval since the first flush)",
                sinceSlowFirst < slowPersistMs, "elapsed=" + Secs(0, sinceSlowFirst));

            slowC.Shutdown();
            consumers.Remove(slowC);
            bool slowSecond = WaitUntil(() => BrokerOffset(admin, gSlow, mq) == 6, 10000, 300);
          Check("I2 the 60s group persists 6 on Shutdown() (the persistConsumerOffset wrap-up)", slowSecond,
                "broker=" + BrokerOffset(admin, gSlow, mq).ToString(CultureInfo.InvariantCulture));
        }
        catch (Exception e)
        {
            Check("the verification threw", false, e.ToString());
        }

        // ---------------- cleanup ----------------
        foreach (DefaultMQPushConsumer c in consumers)
        {
            try
            {
                c.Shutdown();
            }
            catch (Exception e)
            {
          Console.WriteLine("    (consumer shutdown failed: " + e.Message + ")");
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
      Console.WriteLine("    (producer shutdown failed: " + e.Message + ")");
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
            Console.WriteLine("    (producer shutdown failed: " + e.Message + ")");
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
    Console.WriteLine("    (deleteTopic(" + t + ") failed: " + e.Message + ")");
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
  Console.WriteLine("    (deleteSubscriptionGroup(" + g + ") failed: " + e.Message + ")");
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
