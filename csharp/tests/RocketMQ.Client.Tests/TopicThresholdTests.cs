// The two topic-level cache budgets are enforced by DIVISION, not by adding the sibling
// queues up: each enabled budget (-1 = off) is split over the queues this client owns and
// the quotient becomes the matching per-queue knob. That happens on a rebalance round that
// CHANGED the assignment, AFTER this round's per-queue state has been rebuilt, and never at
// pull time (the pull gate only judges one queue's own buffer -- see FlowControlTests).
//
// Every positive leg sits next to a control that goes red when the division is reverted,
// moved, gated wrongly, or fed the wrong number.
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class TopicThresholdTests
{
    private const string Topic = "TopicThresholdNetUnitTopic";
    private const string OtherTopic = "TopicThresholdNetUnitOther";
    private const string Broker = "broker-a";

    /// <summary>The INFO wording the division writes when it overwrites a knob.</summary>
    private const string CountKnobLog = "The pullThresholdForQueue is changed from ";

    private const string SizeKnobLog = "The pullThresholdSizeForQueue is changed from ";

    private static MessageQueue QueueOf(string topic, int id) => new(topic, Broker, id);

    private static string KeyOf(MessageQueue mq) => DefaultMQPushConsumer.OffsetKeyForTest(mq);

    /// <summary>Give the consumer n owned queues of one topic, the way a rebalance round
    /// leaves the per-queue pull-state table (the divisor).</summary>
    private static void Own(DefaultMQPushConsumer c, string topic, int n)
    {
        for (int i = 0; i < n; i++)
        {
            MessageQueue mq = QueueOf(topic, i);
            c.SetAssignedForTest(KeyOf(mq), mq);
        }
    }

    /// <summary>The POP variant of the state table: n queues live in <c>_popQueues</c>.</summary>
    private static void OwnPop(DefaultMQPushConsumer c, string topic, int n)
    {
        for (int i = 0; i < n; i++)
        {
            c.RegisterPopQueueForTest(KeyOf(QueueOf(topic, i)));
        }
    }

    // ---------------------------------------------------------------- INFO line capture

    // ClientLog keeps the write handle for the whole run, and Windows validates share modes
    // in both directions: the file must be opened shared for ReadWrite, the way `tail -f`
    // does it. (Same shape as FlowControlTests' warn capture.) The file is opened lazily on
    // the first line written, so a leg that must log NOTHING has no file at all -- that is
    // the evidence, not an error.
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

    /// <summary>Run <paramref name="work" /> with the log pointed at a private file and
    /// return every INFO line it wrote.</summary>
    /// <remarks>The log file path and the rotation limits are process-global state, and this
    /// assembly runs with parallelisation disabled for exactly that reason: a small limit
    /// left behind by another class would roll these lines into backups that then get
    /// deleted, which reads exactly like "the line was never logged". Raised for this
    /// window, read back to prove it took, and restored in the
    /// <c>finally</c> -- the pattern of
    /// <c>FlowControlTests.WarnCapture_SurvivesASmallGlobalRotationLimit</c>.</remarks>
    private static List<string> CaptureInfo(Action work)
    {
        string dir = Path.Combine(Path.GetTempPath(),
            "rmq_dotnet_tt_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        string path = Path.Combine(dir, "divide.log");
        long sizeBefore = ClientLog.LogFileMaxSize;
        int indexBefore = ClientLog.LogFileMaxIndex;
        try
        {
            ClientLog.SetLogFileLimits(256L * 1024 * 1024, 1);
            // The raised limits must read back, or this capture is measuring a file it does
            // not actually control.
            Assert.Equal(256L * 1024 * 1024, ClientLog.LogFileMaxSize);
            Assert.Equal(1, ClientLog.LogFileMaxIndex);
            ClientLog.SetLogFile(path);
            ClientLog.SetLogLevel(LogLevel.Info);
            work();
            // Prove the capture really reads the file the log writes to: a line written from
            // here must be in it. Without this sentinel an empty result would be
            // indistinguishable from "the log went somewhere else".
            const string Sentinel = "tt-capture-sentinel";
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

    // ---------------------------------------------------------------- the division itself

    /// <summary>An enabled message-count budget divides over the owned queues and ONLY its
    /// own knob moves; the off byte budget leaves its knob and its log line alone.</summary>
    [Fact]
    public void Divide_CountBudget_OnlyTheCountKnobMoves()
    {
        var c = new DefaultMQPushConsumer("GID_tt_divide_count");
        c.PullThresholdForQueue = 999;
        c.PullThresholdSizeForQueue = 50;
        c.PullThresholdForTopic = 1000;
        c.PullThresholdSizeForTopic = -1;
        Own(c, Topic, 4);

        List<string> info = CaptureInfo(c.DivideTopicThresholdsForTest);
        Assert.Equal(250, c.PullThresholdForQueue);
        // Control for "only its own knob": 100/4 would be the byte budget dividing although
        // it is off.
        Assert.Equal(50, c.PullThresholdSizeForQueue);
        Assert.Contains(info, l => l.Contains(CountKnobLog + "999 to 250"));
        Assert.DoesNotContain(info, l => l.Contains(SizeKnobLog));
    }

    /// <summary>An enabled byte budget divides the same way, over the same queue count, and
    /// does not touch the count knob.</summary>
    [Fact]
    public void Divide_ByteBudget_OnlyTheByteKnobMoves()
    {
        var c = new DefaultMQPushConsumer("GID_tt_divide_size");
        c.PullThresholdForQueue = 999;
        c.PullThresholdSizeForQueue = 50;
        c.PullThresholdForTopic = -1;
        c.PullThresholdSizeForTopic = 100;
        Own(c, Topic, 4);

        List<string> info = CaptureInfo(c.DivideTopicThresholdsForTest);
        Assert.Equal(25, c.PullThresholdSizeForQueue);
        Assert.Equal(999, c.PullThresholdForQueue);
        Assert.Contains(info, l => l.Contains(SizeKnobLog + "50 to 25"));
        Assert.DoesNotContain(info, l => l.Contains(CountKnobLog));
    }

    /// <summary>Control for the two legs above: both budgets off means nothing is divided,
    /// so the user's per-queue values survive any queue count. An unconditional division
    /// would overwrite them here.</summary>
    [Fact]
    public void Divide_BothBudgetsOff_KeepsTheUserKnobs()
    {
        var c = new DefaultMQPushConsumer("GID_tt_divide_off");
        c.PullThresholdForQueue = 4242;
        c.PullThresholdSizeForQueue = 43;
        c.PullThresholdForTopic = -1;
        c.PullThresholdSizeForTopic = -1;
        Own(c, Topic, 4);

        List<string> info = CaptureInfo(c.DivideTopicThresholdsForTest);
        Assert.Equal(4242, c.PullThresholdForQueue);
        Assert.Equal(43, c.PullThresholdSizeForQueue);
        Assert.Empty(info);
    }

    /// <summary>A budget smaller than the owned queue count still leaves a usable gate: the
    /// quotient is floored at 1, never 0 (0 would mean "not one message may be cached").</summary>
    [Fact]
    public void Divide_BudgetSmallerThanOwned_FloorsAtOne()
    {
        var c = new DefaultMQPushConsumer("GID_tt_divide_floor");
        c.PullThresholdForQueue = 1000;
        c.PullThresholdSizeForQueue = 100;
        c.PullThresholdForTopic = 3;
        c.PullThresholdSizeForTopic = 1;
        Own(c, Topic, 4);

        CaptureInfo(c.DivideTopicThresholdsForTest);
        Assert.Equal(1, c.PullThresholdForQueue);
        Assert.Equal(1, c.PullThresholdSizeForQueue);

        // Control: the floored knob really is a gate of one message, not a dead consumer.
        MessageQueue mq = QueueOf(Topic, 0);
        c.PullThresholdSizeForQueue = 0;
        c.ConsumeConcurrentlyMaxSpan = 0;
        c.SetPendingForTest(KeyOf(mq), new List<MessageExt> { new() { Topic = Topic, StoreSize = 1 } });
        Assert.False(c.FlowControlHitForTest(mq, KeyOf(mq)), "one message fits a gate of 1");
        c.SetPendingForTest(KeyOf(mq), new List<MessageExt>
        {
            new() { Topic = Topic, StoreSize = 1 }, new() { Topic = Topic, StoreSize = 1 },
        });
        Assert.True(c.FlowControlHitForTest(mq, KeyOf(mq)), "two messages must pause on a gate of 1");
    }

    /// <summary>Owning no queue means there is nothing to divide by: both knobs stay exactly
    /// as the user set them (no division by zero, no quotient of 0).</summary>
    [Fact]
    public void Divide_OwningNoQueue_KeepsBothKnobsAndLogsNothing()
    {
        var c = new DefaultMQPushConsumer("GID_tt_divide_no_queue");
        c.PullThresholdForQueue = 1000;
        c.PullThresholdSizeForQueue = 100;
        c.PullThresholdForTopic = 1000;
        c.PullThresholdSizeForTopic = 100;

        List<string> info = CaptureInfo(c.DivideTopicThresholdsForTest);
        Assert.Equal(1000, c.PullThresholdForQueue);
        Assert.Equal(100, c.PullThresholdSizeForQueue);
        Assert.Empty(info);
    }

    /// <summary>The dividend is always the topic budget, so repeating with an unchanged queue
    /// count writes the same number (and logs nothing), and a grown queue set is divided
    /// from the budget: 1000/8 = 125, not the self-fed 250/8 = 31.</summary>
    [Fact]
    public void Divide_Repeats_AndTracksTheQueueCount()
    {
        var c = new DefaultMQPushConsumer("GID_tt_divide_repeat");
        c.PullThresholdForQueue = 999;
        c.PullThresholdSizeForQueue = 99;
        c.PullThresholdForTopic = 1000;
        c.PullThresholdSizeForTopic = 100;
        Own(c, Topic, 4);

        CaptureInfo(c.DivideTopicThresholdsForTest);
        Assert.Equal(250, c.PullThresholdForQueue);
        Assert.Equal(25, c.PullThresholdSizeForQueue);

        // Idempotence: the same divisor re-derives the same numbers, so it writes nothing
        // and logs nothing.
        List<string> again = CaptureInfo(c.DivideTopicThresholdsForTest);
        Assert.Equal(250, c.PullThresholdForQueue);
        Assert.Equal(25, c.PullThresholdSizeForQueue);
        Assert.Empty(again);

        // Four more queues: the budget is re-split from the budget, not from the quotient
        // the previous round wrote.
        Own(c, Topic, 8);
        CaptureInfo(c.DivideTopicThresholdsForTest);
        Assert.Equal(125, c.PullThresholdForQueue);
        Assert.Equal(12, c.PullThresholdSizeForQueue);
    }

    /// <summary>The divisor counts EVERY queue this client owns, whatever topic it belongs
    /// to -- not the queues of one topic.</summary>
    [Fact]
    public void Divide_CountsQueuesAcrossAllTopics()
    {
        var c = new DefaultMQPushConsumer("GID_tt_divide_topics");
        c.PullThresholdForQueue = 999;
        c.PullThresholdForTopic = 1000;
        // Two queues of the fixture topic plus two of another one.
        Own(c, Topic, 2);
        Own(c, OtherTopic, 2);

        CaptureInfo(c.DivideTopicThresholdsForTest);
        // Control: a per-topic divisor would read 2 here and write 500.
        Assert.Equal(250, c.PullThresholdForQueue);
    }

    /// <summary>POP keeps its per-queue state in its own table, and that is the table the
    /// divisor reads. The two tables disagree on purpose, so the same staged state gives a
    /// different answer per mode.</summary>
    [Fact]
    public void Divide_PopMode_DividesOverThePopTable()
    {
        var c = new DefaultMQPushConsumer("GID_tt_divide_pop");
        c.PullThresholdForQueue = 999;
        c.PullThresholdForTopic = 1000;
        Own(c, Topic, 8);
        OwnPop(c, Topic, 4);

        // Pull-mode control: the same consumer reads the pull table (8) and writes 125.
        CaptureInfo(c.DivideTopicThresholdsForTest);
        Assert.Equal(125, c.PullThresholdForQueue);

        c.PullThresholdForQueue = 999;
        c.PopMode = true;
        CaptureInfo(c.DivideTopicThresholdsForTest);
        // Under POP the live table holds 4 queues, so the budget splits over 4.
        Assert.Equal(250, c.PullThresholdForQueue);
    }

    // ------------------------------------------------- the division in the rebalance round

    private sealed class NoopListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context) => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }

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

    /// <summary>A fake cluster whose per-broker readable queue counts can move: the route
    /// body is computed per request, so one rebalance round can be handed an equal-size swap
    /// or a shrink. Only <see cref="Topic"/> has queues -- the auto-subscribed %RETRY% topic
    /// gets none, so the owned queue count is exactly what this fixture says it is.</summary>
    private sealed class RebalanceCluster : IDisposable
    {
        private readonly string[] _brokers;
        private int[] _queuesPerBroker;

        public MockCluster Cluster { get; }

        public RebalanceCluster(params int[] queuesPerBroker)
        {
            _queuesPerBroker = (int[])queuesPerBroker.Clone();
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
                if (asked == Topic)
                {
                    for (int i = 0; i < _queuesPerBroker.Length; ++i)
                    {
                        if (_queuesPerBroker[i] > 0)
                        {
                            route.QueueDatas.Add(new QueueData(_brokers[i], _queuesPerBroker[i],
                                _queuesPerBroker[i], PermName.PermRead | PermName.PermWrite, 0));
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

        public void SetQueues(params int[] queuesPerBroker) =>
            _queuesPerBroker = (int[])queuesPerBroker.Clone();

        /// <summary>Force the instance to re-read the topic route, so the next round sees the
        /// mutated queue counts (waiting for the route poll would take 30s).</summary>
        public void RefreshRoute(DefaultMQPushConsumer c) =>
            Assert.True(c.Client().UpdateTopicRouteInfoFromNameServer(Topic),
                "the fake name server must answer the route");

        /// <summary>A started push consumer with one topic budget enabled and the other
        /// off, plus per-queue values the division is expected to overwrite.</summary>
        public DefaultMQPushConsumer Start(string group, string instance)
        {
            DefaultMQPushConsumer? consumer = null;
            var c = new DefaultMQPushConsumer(group);
            consumer = c;
            c.SetNamesrvAddr(Cluster.NamesrvAddr);
            c.InstanceName = instance;
            c.Subscribe(Topic, "*");
            c.SetMessageListener(new NoopListener());
            c.PullThresholdForQueue = 999;
            c.PullThresholdSizeForQueue = 50;
            c.PullThresholdForTopic = 1000;
            c.PullThresholdSizeForTopic = -1;
            // Rebalance only hands queues to a consumer it finds in the group's cid list:
            // answer with this client's own id, so a lone instance owns every queue.
            Cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
                _ => CidBody(consumer!.ClientId));
            c.Start();
            return c;
        }

        public void Dispose() => Cluster.Dispose();
    }

    private static bool WaitFor(Func<bool> cond, int millis = 5000)
    {
        DateTime deadline = DateTime.UtcNow + TimeSpan.FromMilliseconds(millis);
        while (DateTime.UtcNow < deadline)
        {
            if (cond()) return true;
            Thread.Sleep(20);
        }

        return cond();
    }

    /// <summary>Shut a started consumer down without paying one whole rebalance period.
    /// </summary>
    /// <remarks>Shutdown joins the rebalance thread, and that thread blocks in
    /// <c>_rebalanceNow.Wait(20000)</c>, which Shutdown's stop flags do not wake: a consumer
    /// that owns queues would make every such test last 20s. Poking the wake-up event while
    /// Shutdown runs shortens only that wait -- the loop re-reads <c>_stop</c> on every wake
    /// and returns -- and needs no production change.</remarks>
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

    /// <summary>The first round that produces an assignment divides, and a round that
    /// changes nothing does not: an off budget never divides (its knob keeps the user
    /// value), and a hand-set per-queue knob survives an unchanged round.</summary>
    [Fact]
    public void Rebalance_DividesOnAChangedRound_SkipsAnUnchangedOne()
    {
        using var f = new RebalanceCluster(2, 2);
        DefaultMQPushConsumer c = f.Start("GID_tt_reb_unchanged", "tt-reb-unchanged");
        try
        {
            // 4 owned queues: 1000 -> 250, written by the round that first assigned them.
            Assert.True(WaitFor(() => c.PullThresholdForQueue == 250),
                "the first assignment must split 1000 over the 4 owned queues, knob="
                + c.PullThresholdForQueue);
            // Control: the byte budget is off, so its knob is exactly the user's 50.
            Assert.Equal(50, c.PullThresholdSizeForQueue);

            // An identical round must NOT divide: only a changed assignment does, so the
            // hand-set value survives. Reverting the gate to "always divide" writes 250
            // here.
            c.PullThresholdForQueue = 777;
            List<string> info = CaptureInfo(c.DoRebalanceForTest);
            Assert.Equal(777, c.PullThresholdForQueue);
            Assert.DoesNotContain(info, l => l.Contains("rebalance result changed"));
            Assert.DoesNotContain(info, l => l.Contains(CountKnobLog));
        }
        finally
        {
            QuickShutdown(c);
        }
    }

    /// <summary>A swap that keeps the queue COUNT must still re-divide (the divisor is the
    /// set, not its size), and a shrink divides by the count THIS round produced -- which
    /// is only visible because the division runs after the pull state was rebuilt.</summary>
    [Fact]
    public void Rebalance_EqualSizeSwapRedivides_ShrinkDividesByTheNewCount()
    {
        using var f = new RebalanceCluster(2, 2);
        DefaultMQPushConsumer c = f.Start("GID_tt_reb_swap", "tt-reb-swap");
        try
        {
            Assert.True(WaitFor(() => c.PullThresholdForQueue == 250), "first round divides");
            Assert.True(c.MqMapContainsForTest(KeyOf(new MessageQueue(Topic, "broker-0", 1))));
            Assert.True(c.MqMapContainsForTest(KeyOf(new MessageQueue(Topic, "broker-1", 1))));

            // 3 queues on the first broker, 1 on the second: still 4 in total, so a count
            // comparison would call this round unchanged and leave the 777 below in place.
            c.PullThresholdForQueue = 777;
            f.SetQueues(3, 1);
            f.RefreshRoute(c);
            List<string> info = CaptureInfo(c.DoRebalanceForTest);
            Assert.Contains(info, l => l.Contains("rebalance result changed")
                                       && l.Contains("assigned=4"));
            Assert.Equal(250, c.PullThresholdForQueue);
            Assert.True(c.MqMapContainsForTest(KeyOf(new MessageQueue(Topic, "broker-0", 2))),
                "the swapped-in queue must be owned");
            Assert.False(c.MqMapContainsForTest(KeyOf(new MessageQueue(Topic, "broker-1", 1))),
                "the swapped-out queue must be gone");

            // 4 -> 2. Doing the division BEFORE RebalancePullThreads would still see the 4
            // queues of the previous round and write 250.
            c.PullThresholdForQueue = 777;
            f.SetQueues(2, 0);
            f.RefreshRoute(c);
            info = CaptureInfo(c.DoRebalanceForTest);
            Assert.Contains(info, l => l.Contains("rebalance result changed")
                                       && l.Contains("assigned=2"));
            Assert.Equal(500, c.PullThresholdForQueue);
            Assert.False(c.MqMapContainsForTest(KeyOf(new MessageQueue(Topic, "broker-0", 2))));
            Assert.False(c.MqMapContainsForTest(KeyOf(new MessageQueue(Topic, "broker-1", 0))));
        }
        finally
        {
            QuickShutdown(c);
        }
    }
}
