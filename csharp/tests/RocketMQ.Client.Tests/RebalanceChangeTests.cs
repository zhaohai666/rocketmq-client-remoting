// What a rebalance round OWES when the assignment actually moved.
//
// The debt is paid per TOPIC, and only for a topic whose assigned queue SET differs from the
// set last round -- a re-ordering of the same queues is not a move, a same-count swap to another
// broker is. For a moved topic the round pays, in this order: re-share the subscription version
// (one wall-clock stamp for the whole round), re-split the topic cache budgets over the queues
// that are alive THIS round, hand every broker one immediate beat carrying the fresh versions
// (and forget the fingerprint table, so no broker answers "unchanged" to a subscription it has
// not seen move), and finally call the application's queue listener with that topic's full route
// set and this instance's share. An unchanged round pays nothing at all -- including a round that
// only heals a stalled pull loop, which keeps the same queue set.
//
// The classic pull consumer owes only the notification half of the package, and the lite consumer
// owes cache deletion on the revoke path; both are pinned here too.
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class RebalanceChangeTests
{
    private const string TopicA = "RebChangeNetTopicA";
    private const string TopicB = "RebChangeNetTopicB";
    private const string GroupPrefix = "GID_RebChange";

    private static MessageQueue Q(string topic, string broker, int id) => new(topic, broker, id);

    private static string KeyOf(MessageQueue mq) => DefaultMQPushConsumer.OffsetKeyForTest(mq);

    // ------------------------------------------------ 1. the moved test, offline

    private static Dictionary<string, HashSet<string>> SetOf(string topic, params MessageQueue[] mqs)
    {
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (MessageQueue mq in mqs) keys.Add(KeyOf(mq));
        return new Dictionary<string, HashSet<string>>(StringComparer.Ordinal) { [topic] = keys };
    }

    /// <summary>The comparison is a SET comparison per topic. Every plausible shortcut --
    /// comparing the total count, comparing the whole table, or comparing the lists as ordered
    /// -- reddens one of these four legs.</summary>
    [Fact]
    public void ChangedTopics_ComparesEachTopicsQueueSet_NotItsSizeOrOrder()
    {
        MessageQueue a0 = Q(TopicA, "broker-0", 0);
        MessageQueue a1 = Q(TopicA, "broker-0", 1);

        // Same set, different order: nothing moved.
        Assert.Empty(DefaultMQPushConsumer.ChangedTopics(SetOf(TopicA, a0, a1), SetOf(TopicA, a1, a0)));

        // A queue gained: the set grew.
        Assert.Equal(new[] { TopicA },
            DefaultMQPushConsumer.ChangedTopics(SetOf(TopicA, a0), SetOf(TopicA, a0, a1)).ToArray());

        // The same COUNT on another broker: a count comparison calls this unchanged, and it is
        // exactly the case that changes which buffers exist.
        Assert.Equal(new[] { TopicA },
            DefaultMQPushConsumer.ChangedTopics(SetOf(TopicA, a0, a1),
                SetOf(TopicA, a0, Q(TopicA, "broker-1", 1))).ToArray());

        // A topic that emptied, and one that appeared, are both changes.
        Assert.Equal(new[] { TopicA },
            DefaultMQPushConsumer.ChangedTopics(SetOf(TopicA, a0),
                new Dictionary<string, HashSet<string>>(StringComparer.Ordinal)).ToArray());
        Assert.Equal(new[] { TopicB },
            DefaultMQPushConsumer.ChangedTopics(
                new Dictionary<string, HashSet<string>>(StringComparer.Ordinal),
                SetOf(TopicB, Q(TopicB, "broker-0", 0))).ToArray());

        // Two topics move: the list is in topic order, and a topic that kept its queues is not in
        // it at all.
        var before = SetOf(TopicA, a0);
        before[TopicB] = new HashSet<string>(StringComparer.Ordinal) { KeyOf(Q(TopicB, "broker-0", 0)) };
        var after = SetOf(TopicA, a0, a1);
        after[TopicB] = new HashSet<string>(StringComparer.Ordinal) { KeyOf(Q(TopicB, "broker-0", 0)) };
        Assert.Equal(new[] { TopicA }, DefaultMQPushConsumer.ChangedTopics(before, after).ToArray());
    }

    // ---------------------------------------------------------------- fake cluster

    /// <summary>A fake cluster whose per-topic / per-broker readable queue counts can move, so
    /// one round can be handed a growth, a shrink or a same-size swap. Only the two fixture
    /// topics have queues -- the auto-subscribed %RETRY% topic gets none, so the owned queue
    /// count is exactly what this fixture says it is.</summary>
    private sealed class ChangeCluster : IDisposable
    {
        private readonly string[] _brokers;
        private readonly Dictionary<string, int[]> _queuesPerTopic = new(StringComparer.Ordinal);

        public MockCluster Cluster { get; }

        public ChangeCluster(params int[] queuesPerBroker)
        {
            Cluster = MockCluster.Start(queuesPerBroker.Length);
            _brokers = new string[queuesPerBroker.Length];
            for (int i = 0; i < _brokers.Length; ++i)
            {
                _brokers[i] = MockCluster.BrokerName(i);
            }

            Cluster.SetRouteBody(req =>
            {
                string asked = req.ExtFields.TryGetValue("topic", out string? t) ? t : "";
                var route = new TopicRouteData();
                if (_queuesPerTopic.TryGetValue(asked, out int[]? counts))
                {
                    for (int i = 0; i < counts.Length && i < _brokers.Length; ++i)
                    {
                        if (counts[i] > 0)
                        {
                            route.QueueDatas.Add(new QueueData(_brokers[i], counts[i], counts[i],
                                PermName.PermRead | PermName.PermWrite, 0));
                        }
                    }
                }

                for (int i = 0; i < _brokers.Length; ++i)
                {
                    route.BrokerDatas.Add(new BrokerData("MockCluster", _brokers[i],
                        new SortedDictionary<long, string>
                        {
                            { MixAll.MasterId, Cluster.BrokerAddrs[i] },
                        }));
                }

                return route.Encode();
            });
        }

        public int BrokerCount => _brokers.Length;

        public string Broker(int i) => _brokers[i];

        /// <summary>The queue counts of one topic, one entry per broker.</summary>
        public void SetQueues(string topic, params int[] countsPerBroker)
        {
            _queuesPerTopic[topic] = (int[])countsPerBroker.Clone();
        }

        /// <summary>Force the instance to re-read a topic's route, so the next round sees the
        /// mutated counts (waiting for the route poll would take 30s).</summary>
        public void RefreshRoute(DefaultMQPushConsumer c, string topic) =>
            Assert.True(c.Client().UpdateTopicRouteInfoFromNameServer(topic),
                "the fake name server must answer the route");

        /// <summary>Start a push consumer subscribed to both topics. The group answers with this
        /// client's own id plus <paramref name="rivalCids"/>, so a rival id is how a case makes
        /// this instance own only part of a topic.</summary>
        public DefaultMQPushConsumer Start(string name, string[] rivalCids)
        {
            string instance = name + "-" + Guid.NewGuid().ToString("N")[..8];
            var c = new DefaultMQPushConsumer(GroupPrefix + "_" + name);
            c.SetNamesrvAddr(Cluster.NamesrvAddr);
            c.InstanceName = instance;
            c.Subscribe(TopicA, "*");
            c.Subscribe(TopicB, "*");
            c.SetMessageListener(new NoopConsumeListener());
            c.PullThresholdForQueue = 999;
            c.PullThresholdSizeForQueue = 50;
            c.PullThresholdForTopic = 1000;
            c.PullThresholdSizeForTopic = -1;
            Cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ =>
                CidBody(new[] { c.ClientId }.Concat(rivalCids).ToArray()));
            c.Start();
            return c;
        }

        public DefaultMQPushConsumer Start(string name) => Start(name, Array.Empty<string>());

        public void Dispose() => Cluster.Dispose();
    }

    private static byte[] CidBody(string[] cids)
    {
        var sb = new StringBuilder("{\"consumerIdList\":[");
        for (int i = 0; i < cids.Length; i++)
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

    private sealed class NoopConsumeListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
                                                       ConsumeConcurrentlyContext context)
            => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }

    /// <summary>Records every queue-listener callback with the two lists it was handed.</summary>
    private sealed class Recorder : IMessageQueueListener
    {
        private readonly List<(string Topic, List<MessageQueue> All, List<MessageQueue> Mine)> _calls = new();

        public void MessageQueueChanged(string topic, IReadOnlyList<MessageQueue> mqAll,
                                       IReadOnlyList<MessageQueue> mqDivided)
        {
            lock (_calls)
            {
                _calls.Add((topic, new List<MessageQueue>(mqAll), new List<MessageQueue>(mqDivided)));
            }
        }

        public List<(string Topic, List<MessageQueue> All, List<MessageQueue> Mine)> Calls
        {
            get
            {
                lock (_calls)
                {
                    return _calls.ToList();
                }
            }
        }

        public int Count
        {
            get
            {
                lock (_calls)
                {
                    return _calls.Count;
                }
            }
        }
    }

    private static bool WaitFor(Func<bool> cond, int millis = 8000)
    {
        long deadline = Environment.TickCount64 + millis;
        while (Environment.TickCount64 < deadline)
        {
            if (cond()) return true;
            Thread.Sleep(20);
        }

        return cond();
    }

    /// <summary>Shut a started consumer down without paying one whole rebalance period: the
    /// rebalance thread blocks in a 20 s wait that the stop flags do not wake.</summary>
    private static void QuickShutdown(DefaultMQPushConsumer c)
    {
        Exception? error = null;
        Thread worker = new(() =>
        {
            try
            {
                c.Shutdown();
            }
            catch (Exception e)
            {
                error = e;
            }
        })
        { IsBackground = true };
        worker.Start();
        while (!worker.Join(50))
        {
            c.WakeupRebalanceForTest();
        }

        if (error is not null)
        {
            throw error;
        }
    }

    // ---------------------------------------------------------------- INFO line capture

    private static string[] ReadAllLinesShared(string path)
    {
        if (!File.Exists(path)) return Array.Empty<string>();
        using FileStream fs = new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using StreamReader sr = new(fs);
        var lines = new List<string>();
        string? line;
        while ((line = sr.ReadLine()) is not null) lines.Add(line);
        return lines.ToArray();
    }

    /// <summary>Run <paramref name="work" /> with the log pointed at a private file and return
    /// every INFO line it wrote. The rotation limits are process-global state and are raised for
    /// this window, then restored; a sentinel line proves the file being read is the one the log
    /// writes to, so an empty result means "nothing was logged", not "the capture is broken".</summary>
    private static List<string> CaptureInfo(Action work)
    {
        string dir = Path.Combine(Path.GetTempPath(),
            "rmq_dotnet_rc_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        string path = Path.Combine(dir, "rebalance.log");
        long sizeBefore = ClientLog.LogFileMaxSize;
        int indexBefore = ClientLog.LogFileMaxIndex;
        try
        {
            ClientLog.SetLogFileLimits(256L * 1024 * 1024, 1);
            ClientLog.SetLogFile(path);
            ClientLog.SetLogLevel(LogLevel.Info);
            work();
            const string Sentinel = "rc-capture-sentinel";
            ClientLog.Info(Sentinel);
            ClientLog.FlushLogFile();
            string[] written = ReadAllLinesShared(path);
            Assert.Contains(written, l => l.Contains(Sentinel));
            return written.Where(l => l.Contains(" INFO  [") && !l.Contains(Sentinel)).ToList();
        }
        finally
        {
            ClientLog.SetLogFile(string.Empty);
            ClientLog.FlushLogFile();
            ClientLog.SetLogLevel(LogLevel.Info);
            ClientLog.SetLogFileLimits(sizeBefore, indexBefore);
            try
            {
                Directory.Delete(dir, recursive: true);
            }
            catch
            {
                // A cleanup failure does not affect the test
            }
        }
    }

    // ---------------------------------------------------------------- 2. the whole package

    /// <summary>A moved topic buys the full package in one round: its version is re-shared, the
    /// topic budget is divided over the queues THIS round owns, and the log states the old and
    /// new version next to the changed assignment.</summary>
    [Fact]
    public void MovedRound_RestampsTheMovedTopic_AndDividesOverThisRoundsQueues()
    {
        using var f = new ChangeCluster(2, 2);
        f.SetQueues(TopicA, 1, 0);
        f.SetQueues(TopicB, 1, 0);
        DefaultMQPushConsumer c = f.Start("package");
        try
        {
            Assert.True(WaitFor(() => c.AssignedQueuesForTest().Count == 2),
                "the fixture needs both topics assigned");
            Assert.Equal(500, c.PullThresholdForQueue);   // 1000 over the 2 queues of round one

            long oldA = c.SubVersionForTest(TopicA);
            long oldB = c.SubVersionForTest(TopicB);
            Assert.True(oldA > 0, "a subscribed topic carries a stamp from the round that assigned it");

            // Topic A gains a queue; topic B is untouched.
            f.SetQueues(TopicA, 2, 1);
            f.RefreshRoute(c, TopicA);
            c.PullThresholdForQueue = 777;
            List<string> info = CaptureInfo(c.DoRebalanceForTest);

            long newA = c.SubVersionForTest(TopicA);
            Assert.True(newA > oldA, "the moved topic must re-share its subscription version");
            // A topic whose queues did not move keeps its version: the broker is allowed to keep
            // the filtered result it cached for it.
            Assert.Equal(oldB, c.SubVersionForTest(TopicB));
            Assert.Contains(info, l => l.Contains(TopicA + " rebalance changed, also update version: "
                                           + oldA + ", " + newA));
            // The division is part of the same package, and its divisor is every queue this
            // round owns -- topic A's three plus topic B's untouched one -- not the previous
            // round's two.
            Assert.Equal(250, c.PullThresholdForQueue);
            Assert.DoesNotContain(info, l => l.Contains(TopicB + " rebalance changed"));
        }
        finally
        {
            QuickShutdown(c);
        }
    }

    /// <summary>One immediate beat per broker, and it carries the fresh version for the moved
    /// topic with the sibling's OLD version untouched on the same wire -- which is what separates
    /// a per-topic re-share from a blanket one, and a rebalance-time beat from the periodic one
    /// that would not arrive for another 30 s.</summary>
    [Fact]
    public void MovedRound_BeatsEveryBrokerOnce_CarryingTheRestampedVersion()
    {
        using var f = new ChangeCluster(2, 2);
        f.SetQueues(TopicA, 1, 0);
        f.SetQueues(TopicB, 0, 1);
        DefaultMQPushConsumer c = f.Start("beats");
        try
        {
            Assert.True(WaitFor(() => c.AssignedQueuesForTest().Count == 2), "both topics assigned");
            long oldA = c.SubVersionForTest(TopicA);
            long oldB = c.SubVersionForTest(TopicB);

            long beatsBefore = c.HeartbeatCount;
            f.SetQueues(TopicA, 2, 0);
            f.RefreshRoute(c, TopicA);
            c.DoRebalanceForTest();

            Assert.Equal(beatsBefore + f.BrokerCount, c.HeartbeatCount);
            Assert.True(c.SubVersionForTest(TopicA) > oldA, "the moved topic was re-shared");

            List<SubscriptionData> subs = LastBeatSubscriptions(f.Cluster);
            Assert.Equal(c.SubVersionForTest(TopicA), SubOf(subs, TopicA).SubVersion);
            Assert.Equal(oldB, SubOf(subs, TopicB).SubVersion);
        }
        finally
        {
            QuickShutdown(c);
        }
    }

    /// <summary>The subscription set of the heartbeat body this cluster last saw.</summary>
    private static List<SubscriptionData> LastBeatSubscriptions(MockCluster cluster)
    {
        WireRecord beat = cluster.Records().Last(r => r.Code == RequestCode.HeartBeat);
        HeartbeatData hb = HeartbeatData.FromJson(Json.Parse(Encoding.UTF8.GetString(beat.Body)));
        Assert.NotEmpty(hb.ConsumerDataSet);
        return hb.ConsumerDataSet[0].SubscriptionDataSet;
    }

    private static SubscriptionData SubOf(List<SubscriptionData> subs, string topic) =>
        subs.First(s => s.Topic == topic);

    // ---------------------------------------------------------------- 3. silence

    /// <summary>The 20 s round re-computes the same assignment over and over. Telling every broker
    /// about a subscription that did not move, on every round, is pure wire noise -- and the
    /// application's listener is a change callback, not a heartbeat. So an unchanged round
    /// restamps nothing, divides nothing, beats nobody and calls nobody.</summary>
    [Fact]
    public void UnchangedRound_RestampsNothing_BeatsNobody_AndNotifiesNobody()
    {
        using var f = new ChangeCluster(2, 2);
        f.SetQueues(TopicA, 1, 1);
        f.SetQueues(TopicB, 1, 0);
        var rec = new Recorder();
        DefaultMQPushConsumer c = f.Start("unchanged");
        try
        {
            Assert.True(WaitFor(() => c.AssignedQueuesForTest().Count == 3), "three queues owned");
            c.SetMessageQueueListener(rec);
            long stampA = c.SubVersionForTest(TopicA);
            long stampB = c.SubVersionForTest(TopicB);
            long beats = c.HeartbeatCount;

            // Hand-set knobs and a listener of its own: everything the round would have touched.
            c.PullThresholdForQueue = 777;
            c.PullThresholdSizeForQueue = 43;
            int calls = rec.Count;
            List<string> info = CaptureInfo(c.DoRebalanceForTest);

            Assert.Equal(stampA, c.SubVersionForTest(TopicA));
            Assert.Equal(stampB, c.SubVersionForTest(TopicB));
            Assert.Equal(beats, c.HeartbeatCount);
            Assert.Equal(calls, rec.Count);
            Assert.Equal(777, c.PullThresholdForQueue);
            Assert.Equal(43, c.PullThresholdSizeForQueue);
            Assert.DoesNotContain(info, l => l.Contains("rebalance result changed"));
            Assert.DoesNotContain(info, l => l.Contains("rebalance changed, also update version"));
        }
        finally
        {
            QuickShutdown(c);
        }
    }

    /// <summary>A stalled pull loop is healed by the next round, but the queue was assigned all
    /// along: the round rebuilds the pull state and stops there. A beat per repaired loop would
    /// put an unchanged consumer's subscriptions on the wire every few seconds.</summary>
    [Fact]
    public void SelfHealRound_RebuildsTheLoop_ButPaysNoBeatAndNoNotification()
    {
        using var f = new ChangeCluster(2, 2);
        f.SetQueues(TopicA, 2, 0);
        f.SetQueues(TopicB, 1, 0);
        var rec = new Recorder();
        DefaultMQPushConsumer c = f.Start("selfheal");
        try
        {
            Assert.True(WaitFor(() => c.AssignedQueuesForTest().Count == 3), "three queues owned");
            c.SetMessageQueueListener(rec);
            MessageQueue stalled = c.AssignedQueuesForTest().First(m => m.Topic == TopicA);
            string key = KeyOf(stalled);
            Assert.True(c.MqMapContainsForTest(key), "the queue needs live pull state to heal");

            // Replace its loop with one that is registered but not running: exactly the
            // "thread exited / never started" half of the stall criterion.
            c.ClearRetiredForTest();
            c.RegisterLoopForTest(key, alive: false);
            long stamp = c.SubVersionForTest(TopicA);
            long beats = c.HeartbeatCount;
            int calls = rec.Count;

            c.DoRebalanceForTest();

            Assert.NotEmpty(c.RetiredQueuesForTest());
            Assert.True(c.MqMapContainsForTest(key),
                "the stalled queue must stay owned and be rebuilt -- the round healed it");
            Assert.Equal(beats, c.HeartbeatCount);
            Assert.Equal(stamp, c.SubVersionForTest(TopicA));
            Assert.Equal(calls, rec.Count);
        }
        finally
        {
            QuickShutdown(c);
        }
    }

    // ---------------------------------------------------------------- 4. the application callback

    /// <summary>The callback is (topic, the topic's FULL route set, this instance's share), once
    /// per moved topic, with the share the round just produced. The rival consumer id is what
    /// makes "full set" and "my share" different numbers, so a notification that handed back the
    /// same list twice would still pass without it.</summary>
    [Fact]
    public void Listener_FiresOncePerMovedTopic_WithTheRouteSetAndThisInstancesShare()
    {
        using var f = new ChangeCluster(2, 2);
        f.SetQueues(TopicA, 2, 2);
        f.SetQueues(TopicB, 1, 0);
        var rec = new Recorder();
        // A rival in the group: this instance owns part of topic A's route, which is the only
        // way "the full route set" and "my share" can be told apart on the callback.
        DefaultMQPushConsumer c = f.Start("notify", new[] { "rival-of-notify" });
        try
        {
            Assert.True(WaitFor(() => c.AssignedQueuesForTest().Any(m => m.Topic == TopicA)),
                "the fixture needs this instance to own part of topic A");
            // Installed after the opening round: this case measures one moved round, not the
            // first-view notification that the start-up round already paid.
            c.SetMessageQueueListener(rec);

            // Topic A's route grows by one queue, which this instance only partly takes.
            f.SetQueues(TopicA, 3, 2);
            f.RefreshRoute(c, TopicA);
            int before = rec.Count;
            c.DoRebalanceForTest();

            List<(string Topic, List<MessageQueue> All, List<MessageQueue> Mine)> since =
                rec.Calls.Skip(before).ToList();
            Assert.Equal(new[] { TopicA }, since.Select(s => s.Topic).ToArray());
            (string, List<MessageQueue> All, List<MessageQueue> Mine) moved = since[0];
            Assert.Equal(5, moved.All.Count);
            Assert.InRange(moved.Mine.Count, 1, moved.All.Count - 1);
            Assert.All(moved.Mine, mq => Assert.Equal(TopicA, mq.Topic));
            // The share the callback carries is the same set the round installed as pull state.
            Assert.Equal(new HashSet<string>(c.AssignedQueuesForTest()
                    .Where(m => m.Topic == TopicA).Select(KeyOf), StringComparer.Ordinal),
                new HashSet<string>(moved.Mine.Select(KeyOf), StringComparer.Ordinal));
        }
        finally
        {
            QuickShutdown(c);
        }
    }

    /// <summary>The broker side of the debt is not optional: with no application listener
    /// installed the moved round still re-shares the version and still beats, so a missing
    /// callback must not short-circuit the rest of the package.</summary>
    [Fact]
    public void NoListenerInstalled_StillRestamps_AndStillBeatsEveryBroker()
    {
        using var f = new ChangeCluster(2, 2);
        f.SetQueues(TopicA, 1, 0);
        f.SetQueues(TopicB, 1, 0);
        DefaultMQPushConsumer c = f.Start("nolistener");
        try
        {
            Assert.True(WaitFor(() => c.AssignedQueuesForTest().Count == 2), "both topics assigned");
            Assert.Null(c.MessageQueueListener);
            long stamp = c.SubVersionForTest(TopicA);
            long beats = c.HeartbeatCount;

            f.SetQueues(TopicA, 2, 0);
            f.RefreshRoute(c, TopicA);
            c.DoRebalanceForTest();

            Assert.True(c.SubVersionForTest(TopicA) > stamp, "the version moved with no listener installed");
            Assert.Equal(beats + f.BrokerCount, c.HeartbeatCount);
            // 1000 over the 3 queues this round owns (topic A's two plus topic B's one).
            Assert.Equal(333, c.PullThresholdForQueue);
        }
        finally
        {
            QuickShutdown(c);
        }
    }

    /// <summary>A listener that throws is logged and must not break the rebalance round: the
    /// version was already re-shared and the beat already sent before the callback runs.</summary>
    private sealed class ThrowingListener : IMessageQueueListener
    {
        public void MessageQueueChanged(string topic, IReadOnlyList<MessageQueue> mqAll,
                                       IReadOnlyList<MessageQueue> mqDivided)
            => throw new InvalidOperationException("the application refused");
    }

    [Fact]
    public void AThrowingListener_NeitherBreaksTheRound_NorRollsBackWhatItAlreadyPaid()
    {
        using var f = new ChangeCluster(2, 2);
        f.SetQueues(TopicA, 1, 0);
        f.SetQueues(TopicB, 1, 0);
        DefaultMQPushConsumer c = f.Start("throws");
        try
        {
            Assert.True(WaitFor(() => c.AssignedQueuesForTest().Count == 2), "both topics assigned");
            c.SetMessageQueueListener(new ThrowingListener());
            long stamp = c.SubVersionForTest(TopicA);
            long beats = c.HeartbeatCount;

            f.SetQueues(TopicA, 2, 0);
            f.RefreshRoute(c, TopicA);
            c.DoRebalanceForTest();          // must not throw out of the round

            Assert.True(c.SubVersionForTest(TopicA) > stamp);
            Assert.Equal(beats + f.BrokerCount, c.HeartbeatCount);
            // And the round still did its own job afterwards.
            Assert.Equal(3, c.AssignedQueuesForTest().Count);
        }
        finally
        {
            QuickShutdown(c);
        }
    }

    // ------------------------------------------------- 5. the classic pull consumer's half

    /// <summary>The classic pull consumer has no subscription version to re-share and no budget to
    /// divide: what it owes an assignment change is the application's callback, once per topic
    /// whose queue SET moved. The very first view only counts when this instance really got
    /// queues.</summary>
    private sealed class RecordingPullListener : IMessageQueueListener
    {
        public List<(string Topic, int All, int Mine)> Calls { get; } = new();

        public bool Throw { get; set; }

        public void MessageQueueChanged(string topic, IReadOnlyList<MessageQueue> mqAll,
                                       IReadOnlyList<MessageQueue> mqDivided)
        {
            Calls.Add((topic, mqAll.Count, mqDivided.Count));
            if (Throw)
            {
                throw new InvalidOperationException("refused");
            }
        }
    }

    [Fact]
    public void PullConsumer_NotifiesOnlyWhenTheTopicsQueueSetChanges()
    {
        var c = new DefaultMQPullConsumer(GroupPrefix + "_notify");
        var rec = new RecordingPullListener();
        c.SetMessageQueueListener(rec);
        MessageQueue a0 = Q(TopicA, "broker-0", 0);
        MessageQueue a1 = Q(TopicA, "broker-0", 1);

        // First view with queues: reported.
        Assert.True(c.NotifyAssignmentChanged(TopicA, new[] { a0, a1 }, new[] { a0 }));
        Assert.Equal(new[] { (TopicA, 2, 1) }, rec.Calls.ToArray());

        // The same share again, in another order: not a change.
        Assert.False(c.NotifyAssignmentChanged(TopicA, new[] { a1, a0 }, new[] { a0 }));
        Assert.Single(rec.Calls);

        // Same count, other broker: a change.
        Assert.True(c.NotifyAssignmentChanged(TopicA, new[] { a0, a1 },
            new[] { a0, Q(TopicA, "broker-1", 1) }));
        Assert.Equal(2, rec.Calls.Count);

        // A different topic is judged on its own history, and its first empty view says nothing.
        Assert.False(c.NotifyAssignmentChanged(TopicB, Array.Empty<MessageQueue>(),
            Array.Empty<MessageQueue>()));
        Assert.Equal(2, rec.Calls.Count);
        // ... and the view after it still reports nothing until queues arrive.
        Assert.False(c.NotifyAssignmentChanged(TopicB, Array.Empty<MessageQueue>(),
            Array.Empty<MessageQueue>()));
        Assert.Equal(2, rec.Calls.Count);
        Assert.True(c.NotifyAssignmentChanged(TopicB, new[] { Q(TopicB, "broker-0", 0) },
            new[] { Q(TopicB, "broker-0", 0) }));
        Assert.Equal(new[] { (TopicA, 2, 1), (TopicA, 2, 2), (TopicB, 1, 1) }, rec.Calls.ToArray());
    }

    /// <summary>The topic-keyed listener and the single listener are different sinks: both hear a
    /// change, the same object hears it once, and a listener that throws is swallowed -- the
    /// caller's queue lookup must not break because an application callback refused.</summary>
    [Fact]
    public void PullConsumer_BothListenerSinksFire_AndAThrowingOneIsSwallowed()
    {
        var c = new DefaultMQPullConsumer(GroupPrefix + "_sinks");
        var perTopic = new RecordingPullListener();
        var global = new RecordingPullListener();
        c.RegisterMessageQueueListener(TopicA, perTopic);
        c.SetMessageQueueListener(global);
        MessageQueue a0 = Q(TopicA, "broker-0", 0);

        Assert.True(c.NotifyAssignmentChanged(TopicA, new[] { a0 }, new[] { a0 }));
        Assert.Single(perTopic.Calls);
        Assert.Single(global.Calls);

        // A topic with no keyed listener still reaches the single one.
        Assert.True(c.NotifyAssignmentChanged(TopicB, new[] { Q(TopicB, "broker-0", 0) },
            new[] { Q(TopicB, "broker-0", 0) }));
        Assert.Single(perTopic.Calls);
        Assert.Equal(2, global.Calls.Count);

        // One object registered both ways must not be called twice for the same change.
        var both = new RecordingPullListener();
        var c2 = new DefaultMQPullConsumer(GroupPrefix + "_sameobject");
        c2.RegisterMessageQueueListener(TopicA, both);
        c2.SetMessageQueueListener(both);
        Assert.True(c2.NotifyAssignmentChanged(TopicA, new[] { a0 }, new[] { a0 }));
        Assert.Single(both.Calls);

        // Throwing listener: the verdict is unchanged and the call does not propagate.
        var angry = new RecordingPullListener { Throw = true };
        var c3 = new DefaultMQPullConsumer(GroupPrefix + "_throws");
        c3.SetMessageQueueListener(angry);
        Assert.True(c3.NotifyAssignmentChanged(TopicA, new[] { a0 }, new[] { a0 }));
        Assert.Single(angry.Calls);
        // and it still remembers what it handed out, so the next identical view is quiet
        Assert.False(c3.NotifyAssignmentChanged(TopicA, new[] { a0 }, new[] { a0 }));
    }

    // ---------------------------------------------------------------- lite consumer's half

    /// <summary>The lite consumer's revoke path is the same debt seen from the cache side: a queue
    /// this instance no longer owns must lose its cached rows AND the accumulator that described
    /// them, or the next round keeps pulling against a backlog nobody can poll away.</summary>
    [Fact]
    public void LiteNarrowing_TakesTheBufferTheAccumulatorAndTheRoundsWithTheQueue()
    {
        var q0 = Q(TopicA, "broker-0", 0);
        var q1 = Q(TopicA, "broker-0", 1);
        var c = new DefaultLitePullConsumer(GroupPrefix + "_lite_revoke");
        c.Assign(new[] { q0, q1 });
        c.PullThresholdForAll = int.MaxValue;
        c.PullThresholdForQueue = 1;
        c.PullThresholdSizeForQueue = int.MaxValue;
        c.ConsumeMaxSpan = long.MaxValue;
        var msgs = Enumerable.Range(0, 3)
            .Select(i => new MessageExt
            {
                Topic = TopicA,
                BrokerName = "broker-0",
                QueueId = 0,
                QueueOffset = i,
                Body = new byte[8],
            })
            .ToList();
        c.AppendToLocalBufferForTest(msgs);
        Assert.True(c.CacheFlowControlHitForTest(q0), "staged over its own budget");

        c.Assign(new[] { q1 });
        Assert.False(c.CacheFlowControlHitForTest(q0),
            "a queue this instance gave up must never be refused by a stale accumulator");
        Assert.Equal(0, c.CachedMessageCountOf(q0));
        Assert.Equal(0, c.PendingPullRoundsOf(q0));
        Assert.Equal(0, c.CachedMessageCount);
    }
}
