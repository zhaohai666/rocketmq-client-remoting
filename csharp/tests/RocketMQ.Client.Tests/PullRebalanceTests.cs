// Pull-side rebalance (Java `DefaultMQPullConsumer.enableRebalance`).
//
// Java's pull consumer is not the "no rebalance" shape this port used to have: the shared
// `RebalanceService` thread calls `mqClientFactory.doRebalance()` every
// `rocketmq.client.rebalance.waitInterval` (20s), which walks the registered consumers and
// calls `tryRebalance()`. That method gates on `enableRebalance` (default **true**) and then
// runs `rebalanceImpl.doRebalance(false)` -> allocate -> `messageQueueChanged(topic, mqAll,
// mqDivided)` (DefaultMQPullConsumerImpl:388-406 / RebalancePullImpl:44-53).
//
// The assertions below pin the three things that are easy to get wrong:
//
//   * `enableRebalance == false` must still report **balanced** (`tryRebalance` returns true
//     at DefaultMQPullConsumerImpl:398-402) -- reporting "unbalanced" would make the loop
//     retry at minInterval forever and burn a round-trip per second for nothing.
//   * the listener fires **only when the allocation changed**, with `(topic, mqAll,
//     mqDivided)` -- and a re-ordering of the same set is not a change.
//   * a throwing listener must not escape: Java logs and swallows it (RebalancePullImpl:49-51)
//     so one bad callback cannot kill the loop and freeze every allocation.
//
// Driven through the real fake cluster rather than a hand-rolled client stub: the route and
// GET_CONSUMER_LIST_BY_GROUP answers are exactly the two inputs a round consumes, and
// MockCluster can script both (default route, SetRouteBody, SetReplyBody).
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class PullRebalanceTests
{
    private const string Topic = "TopicPullReb";
    private const string Group = "GID_pull_rebalance_unit";

    // ---------------------------------------------------------------- helpers

    private static byte[] CidBody(params string[] cids)
    {
        var sb = new StringBuilder("{\"consumerIdList\":[");
        for (int i = 0; i < cids.Length; ++i)
        {
            if (i > 0)
            {
                sb.Append(',');
            }

            sb.Append('"').Append(cids[i]).Append('"');
        }

        sb.Append("]}");
        return Encoding.UTF8.GetBytes(sb.ToString());
    }

    /// <summary>A one-broker route with the requested read-queue count; 0 queues means "the
    /// topic is gone" while the route itself still exists.</summary>
    private static byte[] SingleBrokerRoute(MockCluster cluster, int queues)
    {
        var route = new TopicRouteData();
        route.QueueDatas.Add(new QueueData("broker-0", queues, queues,
            PermName.PermRead | PermName.PermWrite, 0));
        route.BrokerDatas.Add(new BrokerData("MockCluster", "broker-0",
            new SortedDictionary<long, string> { { MixAll.MasterId, cluster.BrokerAddrs[0] } }));
        return route.Encode();
    }

    /// <summary>broker-0 .. broker-(n-1), one queue each; <paramref name="reverse"/> emits the
    /// same SET in the opposite order (a re-ordering, not a change).</summary>
    private static byte[] OrderedRoute(MockCluster cluster, int brokers, bool reverse)
    {
        var route = new TopicRouteData();
        var order = new List<int>();
        for (int i = 0; i < brokers; ++i)
        {
            order.Add(i);
        }

        if (reverse)
        {
            order.Reverse();
        }

        foreach (int i in order)
        {
            string name = MockCluster.BrokerName(i);
            route.QueueDatas.Add(new QueueData(name, 1, 1,
                PermName.PermRead | PermName.PermWrite, 0));
            route.BrokerDatas.Add(new BrokerData("MockCluster", name,
                new SortedDictionary<long, string> { { MixAll.MasterId, cluster.BrokerAddrs[i] } }));
        }

        return route.Encode();
    }

    private sealed class Recorder : IMessageQueueListener
    {
        private readonly List<(string Topic, List<string> All, List<string> Divided)> _calls = new();

        public List<(string Topic, List<string> All, List<string> Divided)> Calls()
        {
            lock (_calls)
            {
                return new List<(string, List<string>, List<string>)>(_calls);
            }
        }

        public void MessageQueueChanged(string topic, IReadOnlyList<MessageQueue> mqAll,
            IReadOnlyList<MessageQueue> mqDivided)
        {
            lock (_calls)
            {
                _calls.Add((topic, Keys(mqAll), Keys(mqDivided)));
            }
        }
    }

    private sealed class ThrowingListener : IMessageQueueListener
    {
        public void MessageQueueChanged(string topic, IReadOnlyList<MessageQueue> mqAll,
            IReadOnlyList<MessageQueue> mqDivided) => throw new InvalidOperationException("listener exploded");
    }

    /// <summary>Wraps the real strategy so a case can prove it was (not) consulted.</summary>
    private sealed class CountingStrategy : IAllocateMessageQueueStrategy
    {
        private readonly IAllocateMessageQueueStrategy _inner = new AllocateMessageQueueAveragely();

        public int Calls { get; private set; }

        public List<MessageQueue> Allocate(string consumerGroup, string currentCid,
            List<MessageQueue> mqAll, List<string> cidAll)
        {
            Calls++;
            return _inner.Allocate(consumerGroup, currentCid, mqAll, cidAll);
        }

        public string GetName() => "COUNTING";
    }

    private static List<string> Keys(IReadOnlyList<MessageQueue> queues) =>
        queues.Select(q => q.BrokerName + ":" + q.QueueId.ToString())
            .OrderBy(k => k, StringComparer.Ordinal).ToList();

    /// <summary>A pull consumer pointed at the fake cluster; <paramref name="listener"/> (or a
    /// silent recorder) registers the topic, which also puts it on the route-refresh list.</summary>
    private static DefaultMQPullConsumer NewConsumer(MockCluster cluster, string instance,
        IMessageQueueListener? listener = null, int pollNameServerMillis = 30000,
        string messageModel = MessageModel.Clustering)
    {
        var consumer = new DefaultMQPullConsumer(Group);
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.SetInstanceName(instance);
        consumer.PollNameServerIntervalMillis = pollNameServerMillis;
        consumer.SetMessageModel(messageModel);
        consumer.RegisterMessageQueueListener(Topic, listener ?? new Recorder());
        return consumer;
    }

    private static bool WaitFor(Func<bool> cond, int timeoutMs = 5000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (cond())
            {
                return true;
            }

            Thread.Sleep(20);
        }

        return cond();
    }

    private static void WithConsumer(DefaultMQPullConsumer consumer, Action<DefaultMQPullConsumer> body)
    {
        consumer.Start();
        try
        {
            body(consumer);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 1. the switch

    [Fact]
    public void EnableRebalance_DefaultsToJavaTrue()
    {
        // Java DefaultMQPullConsumer.java:97 `private boolean enableRebalance = true`.
        Assert.True(new DefaultMQPullConsumer(Group).EnableRebalance);
    }

    [Fact]
    public void EnableRebalance_RoundTrips()
    {
        var consumer = new DefaultMQPullConsumer(Group);
        consumer.EnableRebalance = false;
        Assert.False(consumer.EnableRebalance);
        consumer.EnableRebalance = true;
        Assert.True(consumer.EnableRebalance);
    }

    [Fact]
    public void WaitAndMinInterval_AreConfigurableAndClamped()
    {
        var consumer = new DefaultMQPullConsumer(Group);
        Assert.Equal(20000, consumer.RebalanceWaitMillis);
        Assert.Equal(1000, consumer.RebalanceMinIntervalMillis);

        consumer.RebalanceWaitMillis = 0;
        consumer.RebalanceMinIntervalMillis = -5;
        Assert.Equal(1, consumer.RebalanceWaitMillis);        // a zero period would spin the loop
        Assert.Equal(1, consumer.RebalanceMinIntervalMillis);
    }

    // ---------------------------------------------------------------- 2. switch off

    [Fact]
    public void Disabled_ReportsBalanced_AndNeverAllocates()
    {
        using var cluster = MockCluster.Start(4);
        var strategy = new CountingStrategy();
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-disabled", pollNameServerMillis: 100);
        consumer.SetAllocateMessageQueueStrategy(strategy);
        consumer.EnableRebalance = false;

        WithConsumer(consumer, c =>
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
                _ => CidBody(c.ClientId, "peer-cid"));

            // Java tryRebalance returns **true** when the switch is off (:398-402).
            Assert.True(c.DoRebalance(), "off must look balanced, not endless-unbalanced");
            Assert.Equal(0, strategy.Calls);
            Assert.Empty(c.GetRebalanceTable());
        });
    }

    [Fact]
    public void Disabled_KeepsFetchInBalanceOnTheSpot_AndIgnoresAStaleTable()
    {
        using var cluster = MockCluster.Start(4);
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-disabled-fetch", pollNameServerMillis: 100);

        WithConsumer(consumer, c =>
        {
            // Warm the table while this instance is alone in the group: it owns all 4.
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(c.ClientId));
            c.DoRebalance();
            Assert.Equal(4, c.GetRebalanceTable()[Topic].Count);

            // A peer joins and the switch goes off: the warm table must be ignored and the
            // assignment computed on the spot (2 of 4), exactly the pre-rebalance contract.
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
                _ => CidBody(c.ClientId, "peer-cid"));
            c.EnableRebalance = false;
            Assert.Equal(2, c.FetchMessageQueuesInBalance(Topic).Count);
        });
    }

    // ---------------------------------------------------------------- 3. broadcasting

    [Fact]
    public void Broadcasting_FirstRoundNotifiesWithAllQueues()
    {
        using var cluster = MockCluster.Start(3);
        var rec = new Recorder();
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-bcast", rec,
            pollNameServerMillis: 100, messageModel: MessageModel.Broadcasting);

        WithConsumer(consumer, c =>
        {
            Assert.False(c.DoRebalance(), "the first round changes the empty table");
            (string topic, List<string> all, List<string> divided) = Assert.Single(rec.Calls());
            Assert.Equal(Topic, topic);
            Assert.Equal(new[] { "broker-0:0", "broker-1:0", "broker-2:0" }, all);
            Assert.Equal(all, divided);
        });
    }

    [Fact]
    public void Broadcasting_SecondRoundIsBalancedAndSilent()
    {
        using var cluster = MockCluster.Start(3);
        var rec = new Recorder();
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-bcast-2", rec,
            pollNameServerMillis: 100, messageModel: MessageModel.Broadcasting);

        WithConsumer(consumer, c =>
        {
            c.DoRebalance();
            Assert.True(c.DoRebalance(), "nothing changed, so the topic is balanced");
            Assert.Single(rec.Calls());
        });
    }

    // ---------------------------------------------------------------- 4. clustering

    [Fact]
    public void Clustering_ThisInstanceTakesItsShare()
    {
        using var cluster = MockCluster.Start(4);
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-share", pollNameServerMillis: 100);

        WithConsumer(consumer, c =>
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
                _ => CidBody(c.ClientId, "peer-cid"));
            Assert.False(c.DoRebalance());

            // Averagely over 4 queues and 2 clients: exactly one half, contiguous in the
            // house ordering (which half depends on where this clientId sorts).
            List<string> mine = Keys(c.GetRebalanceTable()[Topic]);
            Assert.Equal(2, mine.Count);
            Assert.True(mine.SequenceEqual(new[] { "broker-0:0", "broker-1:0" })
                        || mine.SequenceEqual(new[] { "broker-2:0", "broker-3:0" }),
                "share=" + string.Join(",", mine));
        });
    }

    [Fact]
    public void Clustering_TwoInstancesGetDisjointShares()
    {
        using var cluster = MockCluster.Start(5);
        DefaultMQPullConsumer a = NewConsumer(cluster, "reb-disjoint-a", pollNameServerMillis: 100);
        DefaultMQPullConsumer b = NewConsumer(cluster, "reb-disjoint-b", pollNameServerMillis: 100);
        a.Start();
        b.Start();
        try
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
                _ => CidBody(a.ClientId, b.ClientId));
            a.DoRebalance();
            b.DoRebalance();

            List<string> sa = Keys(a.GetRebalanceTable()[Topic]);
            List<string> sb = Keys(b.GetRebalanceTable()[Topic]);
            Assert.Empty(sa.Intersect(sb, StringComparer.Ordinal));
            Assert.Equal(5, sa.Concat(sb).Distinct(StringComparer.Ordinal).Count());
        }
        finally
        {
            a.Shutdown();
            b.Shutdown();
        }
    }

    [Fact]
    public void Clustering_NewQueueTriggersExactlyOneNotification()
    {
        using var cluster = MockCluster.Start(1);
        var queues = new[] { 1 };
        cluster.SetRouteBody(_ => SingleBrokerRoute(cluster, queues[0]));
        var rec = new Recorder();
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-newqueue", rec, pollNameServerMillis: 100);

        WithConsumer(consumer, c =>
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(c.ClientId));
            Assert.False(c.DoRebalance());
            Assert.Single(rec.Calls());

            queues[0] = 3;   // a queue was added
            Assert.True(WaitFor(() => c.FetchSubscribeMessageQueues(Topic).Count == 3),
                "the route refresh must see the new queue");

            Assert.False(c.DoRebalance());
            Assert.Equal(2, rec.Calls().Count);
            (_, List<string> all, List<string> divided) = rec.Calls()[1];
            Assert.Equal(new[] { "broker-0:0", "broker-0:1", "broker-0:2" }, all);
            Assert.Equal(all, divided);
        });
    }

    [Fact]
    public void Clustering_MissingConsumerIdList_IsBalancedAndSilent()
    {
        using var cluster = MockCluster.Start(3);
        var rec = new Recorder();
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-nocid", rec, pollNameServerMillis: 100);

        WithConsumer(consumer, c =>
        {
            // No body scripted -> the broker answers an empty body, which reads as "no list".
            // Java (:300-302) only warns: no notify, balanced stays true, table stays empty.
            Assert.True(c.DoRebalance());
            Assert.Empty(rec.Calls());
            Assert.Empty(c.GetRebalanceTable());
        });
    }

    [Fact]
    public void Clustering_VanishedTopicClearsTheTableAndNotifiesEmpty()
    {
        using var cluster = MockCluster.Start(1);
        var queues = new[] { 2 };
        cluster.SetRouteBody(_ => SingleBrokerRoute(cluster, queues[0]));
        var rec = new Recorder();
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-vanished", rec, pollNameServerMillis: 100);

        WithConsumer(consumer, c =>
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(c.ClientId));
            c.DoRebalance();
            Assert.Single(rec.Calls());

            queues[0] = 0;   // the topic disappeared from the route
            Assert.True(WaitFor(() => c.FetchSubscribeMessageQueues(Topic).Count == 0));

            // Java (:276-294) notifies with two empty sets when the route disappears and then
            // `break`s, leaving balanced at its initial true: a transient absence must not pin
            // the loop at minInterval and spin once per second.
            Assert.True(c.DoRebalance(), "a vanished topic is still a balanced round");
            Assert.Empty(c.GetRebalanceTable()[Topic]);
            Assert.Equal(2, rec.Calls().Count);
            (_, List<string> all, List<string> divided) = rec.Calls()[1];
            Assert.Empty(all);
            Assert.Empty(divided);
        });
    }

    [Fact]
    public void ASecondRoundOnAVanishedTopic_IsBalancedAndSilent()
    {
        using var cluster = MockCluster.Start(1);
        cluster.SetRouteBody(_ => SingleBrokerRoute(cluster, 0));
        var rec = new Recorder();
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-vanished-2", rec, pollNameServerMillis: 100);

        WithConsumer(consumer, c =>
        {
            // The table was never non-empty, so there is nothing to revoke and nothing to say;
            // the round is still balanced (Java's `break` without touching `balanced`).
            Assert.True(c.DoRebalance());
            Assert.True(c.DoRebalance());
            Assert.Empty(rec.Calls());
            Assert.Empty(c.GetRebalanceTable()[Topic]);
        });
    }

    // ---------------------------------------------------------------- 5. listener safety

    [Fact]
    public void AThrowingListener_DoesNotEscape()
    {
        using var cluster = MockCluster.Start(2);
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-throws", new ThrowingListener(),
            pollNameServerMillis: 100);

        WithConsumer(consumer, c =>
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(c.ClientId));
            // Java swallows it (RebalancePullImpl:49-51); the call must simply return.
            Assert.False(c.DoRebalance());
            Assert.Equal(2, c.GetRebalanceTable()[Topic].Count);
        });
    }

    [Fact]
    public void SameQueuesInAnotherOrder_DoNotNotify()
    {
        using var cluster = MockCluster.Start(2);
        var reverse = new[] { false };
        cluster.SetRouteBody(_ => OrderedRoute(cluster, 2, reverse[0]));
        var rec = new Recorder();
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-reorder", rec, pollNameServerMillis: 100);

        WithConsumer(consumer, c =>
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(c.ClientId));
            c.DoRebalance();
            Assert.Single(rec.Calls());

            reverse[0] = true;
            Assert.True(WaitFor(() => c.FetchSubscribeMessageQueues(Topic)[0].BrokerName == "broker-1"),
                "the route must come back in the opposite order");

            Assert.True(c.DoRebalance(), "same set, different order is not a change");
            Assert.Single(rec.Calls());
        });
    }

    // ---------------------------------------------------------------- 6. the table is the product

    [Fact]
    public void TableWinsOverTheOnTheSpotComputation()
    {
        using var cluster = MockCluster.Start(4);
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-table-wins", pollNameServerMillis: 100);

        WithConsumer(consumer, c =>
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(c.ClientId));
            c.DoRebalance();
            Assert.Equal(4, c.GetRebalanceTable()[Topic].Count);

            // A peer joins afterwards: the background rebalance's product is authoritative, so
            // the answer does not silently halve behind the caller's back.
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
                _ => CidBody(c.ClientId, "peer-cid"));
            Assert.Equal(4, c.FetchMessageQueuesInBalance(Topic).Count);
        });
    }

    [Fact]
    public void TableAnswer_IsSortedAndIsolatedFromTheCaller()
    {
        using var cluster = MockCluster.Start(2);
        cluster.SetRouteBody(_ => OrderedRoute(cluster, 2, reverse: true));
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-table-copy", pollNameServerMillis: 100);

        WithConsumer(consumer, c =>
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(c.ClientId));
            c.DoRebalance();

            List<MessageQueue> got = c.FetchMessageQueuesInBalance(Topic);
            Assert.Equal(new[] { "broker-0", "broker-1" }, got.Select(m => m.BrokerName));

            got.Clear();
            Assert.Equal(2, c.GetRebalanceTable()[Topic].Count);
        });
    }

    [Fact]
    public void GetRebalanceTable_ReturnsACopy()
    {
        using var cluster = MockCluster.Start(2);
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-snapshot", pollNameServerMillis: 100);

        WithConsumer(consumer, c =>
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(c.ClientId));
            c.DoRebalance();
            Dictionary<string, List<MessageQueue>> snapshot = c.GetRebalanceTable();
            snapshot[Topic].Clear();
            Assert.Equal(2, c.GetRebalanceTable()[Topic].Count);
        });
    }

    [Fact]
    public void UnfilledTopic_StillComputesOnTheSpot()
    {
        using var cluster = MockCluster.Start(4);
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-unfilled", pollNameServerMillis: 100);

        WithConsumer(consumer, c =>
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
                _ => CidBody(c.ClientId, "peer-cid"));
            Assert.Empty(c.GetRebalanceTable());
            Assert.Equal(2, c.FetchMessageQueuesInBalance(Topic).Count);
        });
    }

    [Fact]
    public void FetchMessageQueuesInBalance_IsAPureQuery_AndNeverCallsTheListener()
    {
        using var cluster = MockCluster.Start(4);
        var rec = new Recorder();
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-pure-query", rec, pollNameServerMillis: 100);

        WithConsumer(consumer, c =>
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
                _ => CidBody(c.ClientId, "peer-cid"));
            // Java DefaultMQPullConsumerImpl:120-135 only reads the process-queue table: the
            // listener belongs to the rebalance round, so neither the on-the-spot computation
            // nor a repeated lookup may fire it.
            c.EnableRebalance = false;
            Assert.Equal(2, c.FetchMessageQueuesInBalance(Topic).Count);
            Assert.Equal(2, c.FetchMessageQueuesInBalance(Topic).Count);
            Assert.Empty(rec.Calls());

            // The round itself does notify (once, on the first change).
            c.EnableRebalance = true;
            Assert.False(c.DoRebalance());
            Assert.Single(rec.Calls());
            Assert.True(c.DoRebalance());
            Assert.Single(rec.Calls());
        });
    }

    // ---------------------------------------------------------------- 7. the background loop

    [Fact]
    public void RebalanceLoop_RunsOnItsInterval_AndStopsOnShutdown()
    {
        using var cluster = MockCluster.Start(2);
        var rec = new Recorder();
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-loop", rec, pollNameServerMillis: 100);
        consumer.RebalanceWaitMillis = 100;
        consumer.RebalanceMinIntervalMillis = 100;

        consumer.Start();
        try
        {
            // The registered topic makes the round ask the broker for the group's client ids;
            // more than one such request can only come from repeated rounds.
            Assert.True(WaitFor(() => cluster.RequestsAt(0, RequestCode.GetConsumerListByGroup) >= 2),
                "the rebalance loop must repeat, got="
                + cluster.RequestsAt(0, RequestCode.GetConsumerListByGroup));
        }
        finally
        {
            consumer.Shutdown();
        }

        int after = cluster.RequestsAt(0, RequestCode.GetConsumerListByGroup);
        Thread.Sleep(350);
        Assert.Equal(after, cluster.RequestsAt(0, RequestCode.GetConsumerListByGroup));
    }

    [Fact]
    public void RebalanceLoop_DoesNothingWhileTheSwitchIsOff()
    {
        using var cluster = MockCluster.Start(2);
        var rec = new Recorder();
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-loop-off", rec, pollNameServerMillis: 100);
        consumer.RebalanceWaitMillis = 100;
        consumer.RebalanceMinIntervalMillis = 100;
        consumer.EnableRebalance = false;

        consumer.Start();
        try
        {
            Thread.Sleep(450);
            Assert.Equal(0, cluster.RequestsAt(0, RequestCode.GetConsumerListByGroup));
            Assert.Empty(rec.Calls());
            Assert.Empty(consumer.GetRebalanceTable());
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    /// <summary>Always hands out nothing. It pins Java's
    /// updateProcessQueueTableInRebalance semantics: only table additions/removals are a
    /// change, so "nothing -> nothing" is no change -- an empty opening share must stay
    /// silent and the round counts as balanced (otherwise the loop hammers minInterval).</summary>
    private sealed class EmptyStrategy : IAllocateMessageQueueStrategy
    {
        public List<MessageQueue> Allocate(string consumerGroup, string currentCid,
            List<MessageQueue> mqAll, List<string> cidAll) => new();

        public string GetName() => "EMPTY";
    }

    [Fact]
    public void FirstRoundWithAnEmptyShare_IsSilentAndBalanced()
    {
        using var cluster = MockCluster.Start(1);
        cluster.SetRouteBody(_ => SingleBrokerRoute(cluster, 3));
        var rec = new Recorder();
        DefaultMQPullConsumer consumer = NewConsumer(cluster, "reb-empty-first", rec,
            pollNameServerMillis: 100);
        consumer.SetAllocateMessageQueueStrategy(new EmptyStrategy());

        WithConsumer(consumer, c =>
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(c.ClientId));

            Assert.True(c.DoRebalance(), "nothing -> nothing is a balanced round");
            Assert.Empty(rec.Calls());
            Assert.True(c.GetRebalanceTable().TryGetValue(Topic, out var share)
                        && share.Count == 0, "the table keeps the topic with an empty share");

            // The share appearing later IS a change (the table grew from nothing).
            c.SetAllocateMessageQueueStrategy(new AllocateMessageQueueAveragely());
            Assert.False(c.DoRebalance());
            Assert.Single(rec.Calls());
        });
    }
}
