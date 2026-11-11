// Real-machine verification of the pull-side background rebalance
// (DefaultMQPullConsumer.EnableRebalance).
//
// Java's pull consumer rebalances: DefaultMQPullConsumer.java:97 defaults enableRebalance
// to true, and the shared RebalanceService drives tryRebalance() every 20s, calling
// MessageQueueListener.MessageQueueChanged(topic, mqAll, mqDivided) when the allocation
// changed. A real broker proves what offline mocks cannot:
//
//   L1  two instances of one group split a multi-queue topic -- disjoint shares whose
//       union is the whole topic (only possible if both read the same broker-side
//       consumer list and run the same allocation formula);
//   L2  the listener really fired with mqAll = the topic's full queue set;
//   L3  EnableRebalance=false stops the loop (no callbacks, empty table) while
//       FetchMessageQueuesInBalance keeps computing on the spot.
//
// Usage: rmq pull-rebalance [namesrv]
using System.Globalization;

using RocketMQ.Client;
using RocketMQ.Common;

namespace RocketMQ.Examples;

public static class LivePullRebalance
{
    private static readonly string Stamp =
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    private const int QueueNum = 8;
    // Shorter than Java's 20s so the probe finishes quickly; the loop shape is the point.
    private const int WaitMillis = 3000;
    private const int MinMillis = 500;

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

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    // Records MessageQueueChanged calls. The rebalance fires the listener on its own
    // thread, so the recorder takes a lock and stores copies.
    private sealed class Recorder : IMessageQueueListener
    {
        private readonly object _lock = new();
        public int Calls;
        private IReadOnlyList<MessageQueue> _lastAll = Array.Empty<MessageQueue>();
        private IReadOnlyList<MessageQueue> _lastDivided = Array.Empty<MessageQueue>();

        public void MessageQueueChanged(string topic, IReadOnlyList<MessageQueue> mqAll,
            IReadOnlyList<MessageQueue> mqDivided)
        {
            lock (_lock)
            {
                Calls++;
                _lastAll = mqAll.ToArray();
                _lastDivided = mqDivided.ToArray();
            }
        }

        public int LastAllSize
        {
            get { lock (_lock) { return _lastAll.Count; } }
        }

        public HashSet<string> LastDividedIds
        {
            get
            {
                lock (_lock)
                {
                    return new HashSet<string>(_lastDivided.Select(IdOf));
                }
            }
        }
    }

    // Identity by (BrokerName, QueueId): MessageQueue objects never compare by reference
    // across instances, so the share comparisons must go through the identity tuple.
    private static string IdOf(MessageQueue mq) =>
        mq.BrokerName + "#" + mq.QueueId.ToString(CultureInfo.InvariantCulture);

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        string prefix = "PullRebNet_" + Stamp;
        string topic = prefix + "Topic";
        string group = prefix + "Group";

        Console.WriteLine(prefix + ": pull-side rebalance live (ns=" + namesrv + ", topic="
            + topic + ", " + QueueNum.ToString(CultureInfo.InvariantCulture) + " queues)");

        // Create the topic up front: a consumer gets no route for an uncreated topic
        // (TBW102 is a producer-side fallback only).
        {
            var prep = new DefaultMQProducer("PG_PullRebNet_" + Stamp);
            prep.NamesrvAddr = namesrv;
            prep.Start();
            try
            {
                prep.CreateTopic("TBW102", topic, QueueNum);
            }
            catch (Exception e)
            {
                Console.WriteLine("!! CreateTopic failed: " + e.Message);
            }

            prep.Shutdown();
        }

        // createTopic only reaches the broker; wait until the name server really serves
        // the topic's own route with all 8 read queues.
        {
            var waiter = MakeConsumer(namesrv, prefix + "Wait", group, topic, null, enable: true);
            waiter.Start();
            long deadline = NowMs() + 30000;
            int seen = 0;
            while (NowMs() < deadline)
            {
                try
                {
                    seen = waiter.FetchSubscribeMessageQueues(topic).Count;
                    if (seen >= QueueNum)
                    {
                        break;
                    }
                }
                catch (Exception)
                {
                    // route not there yet
                }

                Thread.Sleep(300);
            }

            waiter.Shutdown();
            if (seen < QueueNum)
            {
                Console.WriteLine("FATAL route for " + topic + " never showed " + QueueNum
                    + " queues (last=" + seen.ToString(CultureInfo.InvariantCulture) + ")");
                return 2;
            }
        }

        Console.WriteLine("  created " + topic + " with "
            + QueueNum.ToString(CultureInfo.InvariantCulture) + " queues");

        var recA = new Recorder();
        var recB = new Recorder();
        var a = MakeConsumer(namesrv, prefix + "InstA", group, topic, recA, enable: true);
        var b = MakeConsumer(namesrv, prefix + "InstB", group, topic, recB, enable: true);
        a.Start();
        b.Start();
        DefaultMQPullConsumer? c = null;
        try
        {
            Console.WriteLine("  started two instances, waiting for the first rebalance rounds...");

            List<MessageQueue> Table(DefaultMQPullConsumer cc) =>
                cc.GetRebalanceTable().TryGetValue(topic, out var share)
                    ? share
                    : new List<MessageQueue>();

            bool WaitUntil(Func<bool> pred, long timeoutMs)
            {
                long deadline = NowMs() + timeoutMs;
                while (NowMs() < deadline)
                {
                    if (pred())
                    {
                        return true;
                    }

                    Thread.Sleep(200);
                }

                return pred();
            }

            // L1: both must end up with a non-empty share of the topic.
            bool got = WaitUntil(() => Table(a).Count > 0 && Table(b).Count > 0, 30000);
            List<MessageQueue> shareA = Table(a);
            List<MessageQueue> shareB = Table(b);
            Console.WriteLine("  INFO share A=" + shareA.Count + " queues, share B=" + shareB.Count + " queues");
            Check("L1-both instances ended up with a share of the topic", got,
                "A=" + shareA.Count + " B=" + shareB.Count);

            var idsA = new HashSet<string>(shareA.Select(IdOf));
            var idsB = new HashSet<string>(shareB.Select(IdOf));
            int overlap = idsA.Count(idsB.Contains);
            Check("L1-the two shares are disjoint (one queue is never owned twice)", overlap == 0,
                "overlap=" + overlap.ToString(CultureInfo.InvariantCulture));
            var union = new HashSet<string>(idsA);
            union.UnionWith(idsB);
            bool full = union.Count == QueueNum;
            for (int i = 0; i < QueueNum && full; i++)
            {
                full = union.Contains("broker-a#" + i.ToString(CultureInfo.InvariantCulture));
            }

            Check("L1-the two shares cover every queue of the topic exactly once", full,
                "union=" + union.Count + " of " + QueueNum.ToString(CultureInfo.InvariantCulture));

            // L2: the listener really ran, and mqAll was the topic's full queue set --
            // proof that the allocation was computed against the real route, not invented
            // locally.
            Check("L2-the allocation change was reported to the listener",
                recA.Calls >= 1 && recB.Calls >= 1, "A calls=" + recA.Calls + " B calls=" + recB.Calls);
            Check("L2-messageQueueChanged carried the topic's full queue set as mqAll",
                recA.LastAllSize == QueueNum, "mqAll=" + recA.LastAllSize);
            HashSet<string> divided = recA.LastDividedIds;
            Check("L2-mqDivided matches what this instance reports as its share",
                divided.Count == idsA.Count && divided.All(idsA.Contains),
                "mqDivided=" + divided.Count + " table=" + idsA.Count);

            // The table is the background rebalance's product, so
            // FetchMessageQueuesInBalance must now answer from it (Java reads the
            // process-queue keys).
            List<MessageQueue> inBalance = a.FetchMessageQueuesInBalance(topic);
            var inBalanceIds = new HashSet<string>(inBalance.Select(IdOf));
            Check("L1-FetchMessageQueuesInBalance answers from the rebalance table",
                inBalanceIds.Count == idsA.Count && inBalanceIds.All(idsA.Contains),
                "got=" + inBalanceIds.Count + " expect=" + idsA.Count);

            // L3: the switch really gates the loop.
            var recC = new Recorder();
            c = MakeConsumer(namesrv, prefix + "InstC", group, topic, recC, enable: false);
            c.Start();
            Console.WriteLine("  started a third instance with EnableRebalance=false, waiting 2 rounds...");
            Thread.Sleep(WaitMillis * 2 + 4000);
            Check("L3-EnableRebalance=false stops the loop: no callback at all", recC.Calls == 0,
                "calls=" + recC.Calls);
            Check("L3-EnableRebalance=false leaves the rebalance table empty",
                c.GetRebalanceTable().Count == 0, "table size=" + c.GetRebalanceTable().Count);
            List<MessageQueue> onTheSpot = c.FetchMessageQueuesInBalance(topic);
            Check("L3-with the loop off, FetchMessageQueuesInBalance still computes on the spot",
                onTheSpot.Count > 0, "got=" + onTheSpot.Count + " queues");
        }
        finally
        {
            foreach (DefaultMQPullConsumer? cc in new[] { c, b, a })
            {
                if (cc is not null)
                {
                    try
                    {
                        cc.Shutdown();
                    }
                    catch
                    {
                        // best effort
                    }
                }
            }
        }

        Console.WriteLine();
        Console.WriteLine(prefix + ": " + _pass + " passed, " + _fail + " failed");
        return _fail > 0 ? 1 : 0;
    }

    private static DefaultMQPullConsumer MakeConsumer(string namesrv, string inst, string group,
        string topic, Recorder? rec, bool enable)
    {
        var consumer = new DefaultMQPullConsumer(group);
        // A distinct instanceName gives a distinct clientId; the clientId is what the
        // broker's GET_CONSUMER_LIST_BY_GROUP returns, so without this both facades would
        // be one client.
        consumer.SetInstanceName(inst);
        consumer.SetNamesrvAddr(namesrv);
        consumer.RebalanceWaitMillis = WaitMillis;
        consumer.RebalanceMinIntervalMillis = MinMillis;
        consumer.RegisterMessageQueueListener(topic, rec);
        consumer.EnableRebalance = enable;
        return consumer;
    }
}
