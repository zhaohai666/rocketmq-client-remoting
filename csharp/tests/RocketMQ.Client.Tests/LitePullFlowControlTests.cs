// Offline + fake-cluster tests for the **lite-pull cache flow-control gates**.
//
// What is guarded here is the local buffer a lite consumer fills when the application is
// slow (or never polls): four budgets -- one for the whole buffer and three per queue --
// must refuse the pull BEFORE the request is built, and a refusal must re-schedule only
// that one queue 50 ms later so the backlog can drain. The verdict is pinned two ways: the
// measurements, counters and log cadence run entirely offline (a staged buffer plus the
// same verdict the pull round calls), and "no request on a refused round / a request at the
// boundary" is read off the wire from the fake broker.
//
// Timing rule for this file: a refusal is asserted through the pause it recorded and the
// request count it produced, never through how long a thread slept.
using System.Diagnostics;
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class LitePullFlowControlTests
{
    private const string Topic = "LiteFlowGateTopic";
    private const string Broker = "broker-flow";
    private const string GroupPrefix = "GID_LiteFlowGate";
    private const int MiB = 1024 * 1024;

    // ---------------- helpers ----------------

    /// <summary>Every numeric assertion in this file goes through a two-argument form plus a
    /// message: this xunit build's <c>Assert.Equal</c> takes no "because" argument.</summary>
    private static void Eq(long expected, long actual, string because) =>
        Assert.True(actual == expected, $"expected {expected}, actual {actual}, because {because}");

    /// <summary>An at-least assertion for the wire cases, where a refused queue re-judges
    /// itself on every round after its deadline, so the counters grow.</summary>
    private static void AtLeast(long least, long actual, string because) =>
        Assert.True(actual >= least, $"expected at least {least}, actual {actual}, because {because}");

    private static MessageQueue OfflineQueue(int queueId) => new(Topic, Broker, queueId);

    /// <summary>The queues the fake route really answers (the cluster puts broker-0 in the
    /// route; the queue id travels in the request header only).</summary>
    private static MessageQueue WireQueue(int queueId) => new(Topic, MockCluster.BrokerName(0), queueId);

    /// <summary>A cached message as the local buffer holds it after a pull: it carries the
    /// queue it belongs to, its queue offset and the stored size the decoder recorded.</summary>
    private static MessageExt Cached(MessageQueue mq, long queueOffset, int storeSize, int bodySize = 8)
        => new()
        {
            Topic = mq.Topic,
            BrokerName = mq.BrokerName,
            QueueId = mq.QueueId,
            QueueOffset = queueOffset,
            StoreSize = storeSize,
            Body = new byte[bodySize],
        };

    private static List<MessageExt> Batch(MessageQueue mq, int count, int storeSize,
        long firstOffset = 0, int bodySize = 8)
        => Enumerable.Range(0, count)
            .Select(i => Cached(mq, firstOffset + i, storeSize, bodySize))
            .ToList();

    /// <summary>An assigned, never started consumer: the gates are judged with nothing on the
    /// wire, the way the push path's flow-control cases pin their verdict.</summary>
    private static DefaultLitePullConsumer Offline(string name, params MessageQueue[] queues)
    {
        var c = new DefaultLitePullConsumer(GroupPrefix + "_" + name);
        c.Assign(queues);
        // Start every gate out of reach; each case opens only the one it judges, so an
        // earlier gate can never mask the branch under test.
        c.PullThresholdForAll = int.MaxValue;
        c.PullThresholdForQueue = int.MaxValue;
        c.PullThresholdSizeForQueue = int.MaxValue;
        c.ConsumeMaxSpan = long.MaxValue;
        return c;
    }

    private static void Stage(DefaultLitePullConsumer c, IReadOnlyList<MessageExt> msgs) =>
        c.AppendToLocalBufferForTest(msgs);

    private static bool Hit(DefaultLitePullConsumer c, MessageQueue mq) => c.CacheFlowControlHitForTest(mq);

    private static bool WaitFor(Func<bool> cond, int timeoutMs = 5000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (cond()) return true;
            Thread.Sleep(20);
        }

        return cond();
    }

    private static int LitePulls(MockCluster cluster) => cluster.CountRequests(RequestCode.LitePullMessage);

    private static int LitePullsOf(MockCluster cluster, int queueId) =>
        cluster.CountRequestsWith(RequestCode.LitePullMessage, "queueId",
            queueId.ToString(CultureInfo.InvariantCulture));

    /// <summary>A lite consumer aimed at the fake endpoint, with every budget but the one
    /// under test taken out of the way -- and deliberately NOT started. A wire case that
    /// judges a cached backlog has to stage it before the first round runs: a round finding an
    /// empty buffer sends its request for real, and then "this queue sent nothing" can never
    /// read zero, however correctly the gate works afterwards.</summary>
    private static DefaultLitePullConsumer Lite(string name, MockCluster cluster,
        params MessageQueue[] queues)
    {
        var c = new DefaultLitePullConsumer(GroupPrefix + "_" + name);
        // One client factory per consumer: the factory is keyed by clientId, so the default
        // instance name would let this consumer join an instance another test started against
        // ITS fake nameserver -- the pulls would then leave through the wrong endpoint and the
        // request counts this file reads off the wire would never move.
        c.SetInstanceName(name + "-" + Guid.NewGuid().ToString("N")[..8]);
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        c.SetAutoCommit(false);
        c.PullThresholdForAll = int.MaxValue;
        c.PullThresholdForQueue = int.MaxValue;
        c.PullThresholdSizeForQueue = int.MaxValue;
        c.ConsumeMaxSpan = long.MaxValue;
        c.Assign(queues.Length > 0 ? queues : new[] { WireQueue(0) });
        return c;
    }

    /// <summary><see cref="Lite"/> with the round loop already running, for the cases whose
    /// verdict comes from the broker answer rather than from a backlog staged beforehand.</summary>
    private static DefaultLitePullConsumer StartedLite(string name, MockCluster cluster,
        params MessageQueue[] queues)
    {
        DefaultLitePullConsumer c = Lite(name, cluster, queues);
        c.Start();
        return c;
    }

    /// <summary>Wait until this queue's cache gate has refused it <paramref name="more" />
    /// further times. This is the time base of the "no request left the client" assertions:
    /// refusals only happen in the pull rounds, so a growing counter says rounds ran, and it
    /// also lets any request that was already on the wire land before the count is read.</summary>
    private static bool WaitRefusals(DefaultLitePullConsumer c, long from, int more) =>
        WaitFor(() => c.FlowControlForQueueTriggered >= from + more);

    // The warn lines are read back from the log file the gate itself writes, with the same
    // rotation-limit care the push path's capture takes: a small limit left by another class
    // would roll the early trips into backups that then get deleted, which reads as "no
    // warn".
    private static string[] ReadAllLinesShared(string path)
    {
        using FileStream fs = new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using StreamReader sr = new(fs);
        var lines = new List<string>();
        string? line;
        while ((line = sr.ReadLine()) is not null) lines.Add(line);
        return lines.ToArray();
    }

    private sealed class Captured
    {
        public List<string> Warns { get; init; } = new();
        public List<string> Debugs { get; init; } = new();
    }

    /// <summary>Run <paramref name="rounds" /> gate verdicts with the log pointed at a
    /// private file and hand back the flow-control lines they wrote.</summary>
    private static Captured CaptureGateLines(DefaultLitePullConsumer c, MessageQueue mq, int rounds)
    {
        string dir = Path.Combine(Path.GetTempPath(),
            "rmq_dotnet_lite_fc_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        string path = Path.Combine(dir, "flow.log");
        long sizeBefore = ClientLog.LogFileMaxSize;
        int indexBefore = ClientLog.LogFileMaxIndex;
        try
        {
            ClientLog.SetLogFileLimits(256L * 1024 * 1024, 1);
            ClientLog.SetLogFile(path);
            ClientLog.SetLogLevel(LogLevel.Debug);
            for (int i = 0; i < rounds; i++)
            {
                Assert.True(Hit(c, mq), $"round {i + 1} must refuse the pull");
            }

            ClientLog.FlushLogFile();
            string[] lines = ReadAllLinesShared(path);
            return new Captured
            {
                // The log writer pads the level name to five characters, so DEBUG is followed
                // by exactly one space before the pid bracket while WARN gets two.
                Warns = lines.Where(l => l.Contains(" WARN  [") && l.Contains("cache flow control")).ToList(),
                Debugs = lines.Where(l => l.Contains(" DEBUG [") && l.Contains("cache flow control")).ToList(),
            };
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
            catch (Exception)
            {
                // A cleanup failure does not affect the test
            }
        }
    }

    // ---------------------------------------------------------------- 1. the four knobs

    [Fact]
    public void Defaults_AreTheFourBudgets_AndStayExactlyAsWritten()
    {
        var c = new DefaultLitePullConsumer(GroupPrefix + "_Defaults");
        Eq(10000, c.PullThresholdForAll, "the whole-buffer budget");
        Eq(1000, c.PullThresholdForQueue, "one queue's cached message count");
        Eq(100, c.PullThresholdSizeForQueue, "one queue's cached MiB");
        Eq(2000, c.ConsumeMaxSpan, "one queue's cached offset span");

        // Plain budgets: nothing is rounded up, and a threshold of 0 has to mean "trip on
        // the first cached message" rather than "one message is always allowed".
        c.PullThresholdForQueue = 0;
        Eq(0, c.PullThresholdForQueue, "no clamping on the count knob");
        c.PullThresholdForAll = -5;
        Eq(-5, c.PullThresholdForAll, "no clamping on the aggregate knob");

        // Control: with nothing cached even a 0 threshold lets the round through, and a
        // single cached message trips it. The queue is assigned first, because a gate only
        // ever judges a queue this instance owns.
        MessageQueue mq = OfflineQueue(0);
        c.Assign(new[] { mq });
        c.PullThresholdForAll = int.MaxValue;
        Assert.False(Hit(c, mq), "an empty buffer cannot trip a count threshold of 0");
        Stage(c, Batch(mq, 1, 64));
        Assert.True(Hit(c, mq), "0 means trip on the first cached message");

        // The two parks are the shared constants of this repo, not values invented here.
        Eq(50, DefaultLitePullConsumer.CacheFlowControlBackoffMillis,
            "a cache refusal re-schedules 50 ms later");
        Eq(20, DefaultLitePullConsumer.BrokerFlowControlBackoffMillis,
            "a broker FLOW_CONTROL(215) answer waits 20 ms");
        Assert.NotEqual(DefaultLitePullConsumer.CacheFlowControlBackoffMillis,
            DefaultLitePullConsumer.BrokerFlowControlBackoffMillis);
    }

    // ---------------------------------------------------------------- 2. strict boundary, on the wire

    /// <summary>Exactly <c>pullThresholdForQueue</c> cached messages still sends the request;
    /// one message more sends nothing and records the 50 ms re-schedule. Recovery is asserted
    /// too: once the application polls the buffer dry the next round pulls again, so a gate
    /// cannot deadlock the consumer.</summary>
    [Fact]
    public void CountBoundary_AtThresholdPulls_OneMoreRefuses_AndPollingDryResumes()
    {
        using var cluster = MockCluster.Start(1);
        MessageQueue mq = WireQueue(0);
        var c = new DefaultLitePullConsumer(GroupPrefix + "_CountBoundary");
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        c.SetAutoCommit(false);
        c.PullThresholdForAll = int.MaxValue;
        c.PullThresholdSizeForQueue = int.MaxValue;
        c.ConsumeMaxSpan = long.MaxValue;
        c.PullThresholdForQueue = 3;
        c.Assign(new[] { mq });

        // Three messages against a threshold of 3 is not "greater than 3".
        Stage(c, Batch(mq, 3, 128));
        Eq(3, c.CachedMessageCountOf(mq), "the staged backlog is what the gate measures");
        c.Start();
        try
        {
            Assert.True(WaitFor(() => LitePulls(cluster) >= 1),
                "a queue sitting exactly on the count threshold still pulls");
            Eq(DefaultLitePullConsumer.NoPullPause, c.LastPullPauseMillis(mq),
                "and the boundary round armed no pause at all");

            // One more cached message and the round is refused: no request leaves the
            // client, and what the refusal recorded is the 50 ms cache park.
            Stage(c, Batch(mq, 1, 128, firstOffset: 3));
            Assert.True(WaitFor(() => c.LastPullPauseMillis(mq) == 50),
                "the refusal parked this queue for 50 ms");
            // Let the refusals run before reading the request count: the boundary round that
            // was still allowed could otherwise land after the read and look like a refusal
            // that sent a request.
            Assert.True(WaitRefusals(c, c.FlowControlForQueueTriggered, 5),
                "the queue keeps re-judging itself over the same backlog");
            int frozen = LitePulls(cluster);
            Assert.True(WaitRefusals(c, c.FlowControlForQueueTriggered, 5),
                "and five more rounds passed while it stayed parked");
            Eq(frozen, LitePulls(cluster), "a refused round sends nothing: no pull reached the broker");
            AtLeast(1, c.FlowControlForQueueTriggered, "each refused round counts exactly one");
            Eq(0, c.FlowControlForAllTriggered, "the aggregate gate never fired here");
            Eq(0, c.FlowControlForSpanTriggered, "the span gate never fired here");
            Eq(0, c.PullCursorOf(mq), "the refusal left the pull cursor alone");
            Eq(-1, c.ConsumeCursorOf(mq), "and committed nothing");

            // Recovery: the application drains the buffer and the next round pulls again --
            // a parked queue is re-scheduled, not stopped.
            while (c.CachedMessageCount > 0) c.Poll(200);
            Assert.True(WaitFor(() => LitePulls(cluster) > frozen),
                "after the buffer drains the queue pulls again (no deadlock)");
        }
        finally
        {
            c.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 3. per queue, never summed, on the wire

    /// <summary>A hot queue over its count threshold is refused while its cold sibling keeps
    /// pulling; symmetrically for the size gate. The gates judge one queue's own backlog and
    /// never add a neighbour's to it.</summary>
    [Fact]
    public void PerQueueGates_RefuseTheHotQueue_WhileTheColdSiblingIsPulled()
    {
        MessageQueue hot = WireQueue(0);
        MessageQueue cold = WireQueue(1);

        // Count gate: four cached messages on the hot queue against a budget of 3, the cold
        // queue holding nothing. The backlog is staged before the loop starts, so the very
        // first round already judges it.
        using (var cluster = MockCluster.Start(1))
        {
            DefaultLitePullConsumer c = Lite("PerQueueCount", cluster, hot, cold);
            c.PullThresholdForQueue = 3;
            Stage(c, Batch(hot, 4, 128));
            try
            {
                c.Start();
                Assert.True(WaitFor(() => c.LastPullPauseMillis(hot) == 50),
                    "the hot queue's own backlog refused its round");
                // The sibling's pulls are the clock: several rounds passed on it, and the hot
                // queue reached the broker in none of them.
                Assert.True(WaitFor(() => LitePullsOf(cluster, 1) >= 3),
                    "the cold sibling is pulled while the hot queue is refused");
                Eq(0, LitePullsOf(cluster, 0),
                    "the hot queue's own backlog refused its round; a sibling's state never enters the verdict");
            }
            finally
            {
                c.Shutdown();
            }
        }

        // Size gate: one 2 MiB store record on the hot queue against a budget of 1 MiB,
        // while the cold queue caches three times as many messages.
        using (var cluster = MockCluster.Start(1))
        {
            DefaultLitePullConsumer c = Lite("PerQueueSize", cluster, hot, cold);
            c.PullThresholdSizeForQueue = 1;
            Stage(c, Batch(cold, 3, 64));
            Stage(c, new List<MessageExt> { Cached(hot, 0, 2 * MiB, bodySize: 4) });
            try
            {
                c.Start();
                Assert.True(WaitFor(() => c.LastPullPauseMillis(hot) == 50),
                    "the queue that really holds the bytes was refused");
                Assert.True(WaitFor(() => LitePullsOf(cluster, 1) >= 3),
                    "the sibling with 3 tiny messages keeps pulling: its own 0 MiB is the measure");
                Eq(0, LitePullsOf(cluster, 0), "and the byte-over queue sent nothing");
            }
            finally
            {
                c.Shutdown();
            }
        }
    }

    // ---------------------------------------------------------------- 4. stored size, not body length

    /// <summary>The size gate reads the stored size the decoder records, never the body
    /// length: a small body inside a large store record refuses, and the mirror case (large
    /// body, small store record) must not.</summary>
    [Fact]
    public void SizeGate_MeasuresStoreSize_AndNeverTheBodyLength()
    {
        MessageQueue mq = OfflineQueue(0);
        var c = Offline("StoreSize", mq);
        c.PullThresholdSizeForQueue = 1;

        // A 4-byte body inside a 2 MiB store record: a body-length measurement would report
        // 0 MiB and let the round through.
        Stage(c, new List<MessageExt> { Cached(mq, 0, 2 * MiB, bodySize: 4) });
        Eq(2 * MiB, c.CachedBytesOf(mq), "the accumulator sums storeSize, not body length");
        Assert.True(Hit(c, mq), "the large store record is over the 1 MiB budget");
        Eq(1, c.FlowControlForQueueTriggered, "the size gate shares the count gate's counter");

        // Mirror case: a 5 MiB body whose store record is tiny must not refuse -- the
        // control a body-length implementation gets wrong in the other direction.
        var cold = Offline("StoreSizeControl", mq);
        cold.PullThresholdSizeForQueue = 1;
        Stage(cold, new List<MessageExt> { Cached(mq, 0, 64, bodySize: 5 * MiB) });
        Eq(64, cold.CachedBytesOf(mq), "only the stored bytes are counted");
        Assert.False(Hit(cold, mq), "a large body inside a small store record stays inside the budget");
        Eq(0, cold.FlowControlForQueueTriggered, "a passing round counts nothing");
    }

    // ---------------------------------------------------------------- 5. integer MiB division

    [Fact]
    public void SizeGate_DividesByWholeMib_BelowAtAndAboveTheThreshold()
    {
        MessageQueue mq = OfflineQueue(0);

        // Below 1 MiB x threshold: inside the budget.
        var below = Offline("MibBelow", mq);
        below.PullThresholdSizeForQueue = 1;
        Stage(below, Batch(mq, 3, 300 * 1024));           // 900 KiB
        Eq(0, below.CachedBytesOf(mq) / MiB, "900 KiB is 0 whole MiB");
        Assert.False(Hit(below, mq), "a backlog under one MiB against a 1 MiB budget pulls");

        // Exactly at the threshold in whole MiB: strictly greater is required.
        var exact = Offline("MibExact", mq);
        exact.PullThresholdSizeForQueue = 1;
        Stage(exact, new List<MessageExt> { Cached(mq, 0, MiB) });
        Assert.False(Hit(exact, mq), "exactly 1 MiB against a 1 MiB threshold must not refuse (>, not >=)");

        // One byte over a full MiB: integer division still reads 1 whole MiB, which is not
        // strictly greater than 1 -- the point of dividing before comparing.
        var justOver = Offline("MibPlusOneByte", mq);
        justOver.PullThresholdSizeForQueue = 1;
        Stage(justOver, new List<MessageExt> { Cached(mq, 0, MiB + 1) });
        Assert.False(Hit(justOver, mq), "1 MiB + 1 byte is still 1 whole MiB, so the round goes out");

        // A full MiB above the threshold refuses, on the shared count+size counter.
        var over = Offline("MibPlusWhole", mq);
        over.PullThresholdSizeForQueue = 1;
        Stage(over, new List<MessageExt> { Cached(mq, 0, 2 * MiB) });
        Assert.True(Hit(over, mq), "2 whole MiB is strictly greater than the 1 MiB budget");
        Eq(1, over.FlowControlForQueueTriggered, "the refusal counted on the shared count+size counter");
        Eq(0, over.FlowControlForAllTriggered, "and on no other counter");

        // And the size gate is this queue's own bytes: a sibling's MiB never joins in.
        var pair = Offline("MibSiblings", OfflineQueue(0), OfflineQueue(1));
        pair.PullThresholdSizeForQueue = 1;
        Stage(pair, new List<MessageExt> { Cached(OfflineQueue(0), 0, 2 * MiB) });
        Assert.True(Hit(pair, OfflineQueue(0)), "queue 0 is over its own byte budget");
        Assert.False(Hit(pair, OfflineQueue(1)), "queue 1 has 0 bytes of its own and still pulls");
    }

    // ---------------------------------------------------------------- 6. span gate

    [Fact]
    public void SpanGate_AtMaxSpanPulls_OneMoreRefuses_AndReadsOnlyThisQueue()
    {
        MessageQueue mq = OfflineQueue(0);

        // Offsets 0 and 100 cached: span exactly 100 is not "greater than 100".
        var at = Offline("Span", mq);
        at.ConsumeMaxSpan = 100;
        Stage(at, new List<MessageExt> { Cached(mq, 0, 64), Cached(mq, 100, 64) });
        Eq(100, at.CachedOffsetSpanOf(mq), "span is the highest cached offset minus the lowest");
        Assert.False(Hit(at, mq), "a span of exactly consumeMaxSpan still pulls");
        Eq(0, at.FlowControlForSpanTriggered, "a passing round counts nothing");

        // One offset more refuses, on the span counter alone.
        var over = Offline("SpanOver", mq);
        over.ConsumeMaxSpan = 100;
        Stage(over, new List<MessageExt> { Cached(mq, 0, 64), Cached(mq, 101, 64) });
        Assert.True(Hit(over, mq), "span 101 over a max span of 100 refuses");
        Eq(1, over.FlowControlForSpanTriggered, "the span gate has its own counter");
        Eq(0, over.FlowControlForQueueTriggered, "neither the count nor the size gate was consulted");

        // The span is this queue's own offsets only: a sibling cached 9000 apart does not
        // make this queue's span too long, and an unordered cache still measures max-min.
        MessageQueue q0 = OfflineQueue(0);
        MessageQueue q1 = OfflineQueue(1);
        var pair = Offline("SpanSiblings", q0, q1);
        pair.ConsumeMaxSpan = 100;
        Stage(pair, new List<MessageExt> { Cached(q0, 50, 64), Cached(q0, 5, 64) });
        Stage(pair, new List<MessageExt> { Cached(q1, 0, 64), Cached(q1, 9000, 64) });
        Assert.False(Hit(pair, q0), "a sibling's 9000-offset span is not this queue's span");
        Eq(45, pair.CachedOffsetSpanOf(q0), "and an unordered cache measures max-min, not first-minus-last");
        Assert.True(Hit(pair, q1), "the sibling that really is over the span is the one refused");

        // A single cached message (like an empty cache) has span 0, never a negative one.
        var single = Offline("SpanSingle", mq);
        single.ConsumeMaxSpan = 0;
        Stage(single, Batch(mq, 1, 64, firstOffset: 77));
        Eq(0, single.CachedOffsetSpanOf(mq), "one cached message spans 0");
        Assert.False(Hit(single, mq), "span 0 does not exceed a max span of 0");
    }

    // ---------------------------------------------------------------- 7. aggregate gate

    /// <summary>The whole-buffer budget is a separate budget: the same total that is over
    /// <c>pullThresholdForAll</c> sits inside every queue's own count budget, and the
    /// identical total against a threshold one higher releases every queue again.</summary>
    [Fact]
    public void AggregateGate_SumsEveryQueue_AndIsItsOwnBudget()
    {
        MessageQueue q0 = OfflineQueue(0);
        MessageQueue q1 = OfflineQueue(1);
        MessageQueue q2 = OfflineQueue(2);
        MessageQueue[] allQueues = { q0, q1, q2 };
        var c = new DefaultLitePullConsumer(GroupPrefix + "_Aggregate");
        c.Assign(allQueues);
        c.PullThresholdForQueue = 1000;
        c.PullThresholdSizeForQueue = int.MaxValue;
        c.ConsumeMaxSpan = long.MaxValue;
        c.PullThresholdForAll = 5;

        Stage(c, Batch(q0, 2, 64));
        Stage(c, Batch(q1, 2, 64, firstOffset: 100));
        Stage(c, Batch(q2, 2, 64, firstOffset: 200));
        Eq(6, c.CachedMessageCount, "the aggregate measures the whole buffer");

        // 6 cached against a budget of 5 refuses every queue, although no single queue is
        // anywhere near its own count threshold -- which is what proves the aggregate is a
        // separate budget and not a re-labelled per-queue gate.
        foreach (MessageQueue mq in allQueues)
        {
            Assert.True(Hit(c, mq), $"{mq} is refused by the whole-buffer budget");
            Eq(2, c.CachedMessageCountOf(mq), $"{mq} itself is 2 against its own budget of 1000");
        }

        Eq(3, c.FlowControlForAllTriggered, "the aggregate counter took exactly one per refusal");
        Eq(0, c.FlowControlForQueueTriggered, "the per-queue count gate never fired");
        Eq(0, c.FlowControlForSpanTriggered, "the span gate never fired either");

        // Control: the identical total against a budget of exactly 6 still pulls.
        var exact = new DefaultLitePullConsumer(GroupPrefix + "_AggregateExact");
        exact.Assign(allQueues);
        exact.PullThresholdForQueue = 1000;
        exact.PullThresholdSizeForQueue = int.MaxValue;
        exact.ConsumeMaxSpan = long.MaxValue;
        exact.PullThresholdForAll = 6;
        Stage(exact, Batch(q0, 2, 64));
        Stage(exact, Batch(q1, 2, 64, firstOffset: 100));
        Stage(exact, Batch(q2, 2, 64, firstOffset: 200));
        foreach (MessageQueue mq in allQueues)
        {
            Assert.False(Hit(exact, mq), $"{mq}: 6 cached against a threshold of 6 must not refuse (>, not >=)");
        }

        Eq(0, exact.FlowControlForAllTriggered, "a passing round counts nothing");
    }

    /// <summary>The judgment order is fixed -- aggregate, then count, then size, then span --
    /// and an earlier hit masks the later ones: one refusal increments exactly one
    /// counter.</summary>
    [Fact]
    public void GateOrder_JudgesAggregateFirst_ThenCount_ThenSize_ThenSpan()
    {
        MessageQueue mq = OfflineQueue(0);

        // Aggregate over budget while the count gate is over budget too: only the aggregate
        // counter moves.
        var all = Offline("OrderAll", mq);
        all.PullThresholdForAll = 1;
        all.PullThresholdForQueue = 1;
        Stage(all, Batch(mq, 3, 64));
        Assert.True(Hit(all, mq));
        Eq(1, all.FlowControlForAllTriggered, "the aggregate gate is judged first");
        Eq(0, all.FlowControlForQueueTriggered, "and it masks the count gate that is also over budget");

        // Count over budget while the size gate is over budget too: the shared counter moves
        // once, and the size gate contributes nothing extra.
        var count = Offline("OrderCount", mq);
        count.PullThresholdForQueue = 1;
        count.PullThresholdSizeForQueue = 0;
        Stage(count, Batch(mq, 3, 2 * MiB));
        Assert.True(Hit(count, mq));
        Eq(1, count.FlowControlForQueueTriggered, "the count gate is judged before the size gate");
        Eq(0, count.FlowControlForAllTriggered, "and the refusal was counted once, not once per gate");

        // Only the span left over budget: its own counter, and no borrowed one.
        var span = Offline("OrderSpan", mq);
        span.ConsumeMaxSpan = 10;
        Stage(span, new List<MessageExt> { Cached(mq, 0, 64), Cached(mq, 11, 64) });
        Assert.True(Hit(span, mq));
        Eq(1, span.FlowControlForSpanTriggered, "the span gate is judged last");
        Eq(0, span.FlowControlForQueueTriggered, "and it does not touch the count/size counter");

        // Size-only refusal (count and span comfortably inside): the shared counter again.
        var size = Offline("OrderSize", mq);
        size.PullThresholdSizeForQueue = 1;
        Stage(size, Batch(mq, 2, 2 * MiB));
        Assert.True(Hit(size, mq));
        Eq(1, size.FlowControlForQueueTriggered,
            "count and size share one counter: the size refusal landed on the same total");
        Eq(0, size.FlowControlForSpanTriggered, "and on no other counter");
    }

    // ---------------------------------------------------------------- 8. counters, cadence, wording

    [Fact]
    public void WarnCadence_IsTripOneAndEveryThousandthAfter_OnEachCounter()
    {
        // One rule for all of this repo's flow-control paths: warn on trips 1, 1001, 2001 ...
        foreach (long n in new[] { 1L, 1001L, 2001L })
        {
            Assert.True(DefaultLitePullConsumer.IsFlowControlWarnTrip(n), $"trip {n} must warn");
        }

        foreach (long n in new[] { 0L, 2L, 999L, 1000L, 1002L, 2000L })
        {
            Assert.False(DefaultLitePullConsumer.IsFlowControlWarnTrip(n), $"trip {n} must stay quiet");
        }

        MessageQueue mq = OfflineQueue(0);

        // Count gate, driven directly as the push cases drive theirs: 1000 refusals write
        // exactly one warn (trip 1), trip 1000 is debug-only, and the 1001st warns again.
        var c = Offline("CadenceCount", mq);
        c.PullThresholdForQueue = 1;
        Stage(c, Batch(mq, 2, 64));
        Captured first = CaptureGateLines(c, mq, 1000);
        Eq(1000, c.FlowControlForQueueTriggered, "every refused round counted exactly one");
        Eq(1, first.Warns.Count, "the cadence is one warn per thousand trips, not one per round");
        Assert.Contains("flowControlTimes=1", first.Warns[0]);
        Eq(1000, first.Debugs.Count, "every refusal logs at debug, trip 1000 included");
        Assert.Contains(first.Debugs, l => l.Contains("flowControlTimes=1000"));
        Assert.DoesNotContain(first.Warns, l => l.Contains("flowControlTimes=1000"));

        Captured next = CaptureGateLines(c, mq, 1);
        Eq(1001, c.FlowControlForQueueTriggered, "the 1001st refusal");
        Eq(1, next.Warns.Count, "and it is the second warn");
        Assert.Contains("flowControlTimes=1001", next.Warns[0]);

        // The aggregate gate keeps its own cadence on its own counter, and its warn names the
        // request-count threshold in messages.
        var all = Offline("CadenceAll", mq);
        all.PullThresholdForAll = 1;
        Stage(all, Batch(mq, 2, 64));
        // Quietly reach trip 1000 outside the capture, so the captured round is the 1001st.
        for (int i = 2; i <= 1001; i++)
        {
            Assert.True(Hit(all, mq), $"aggregate round {i} must still refuse");
        }

        Captured allWarn = CaptureGateLines(all, mq, 1);
        Eq(1001, all.FlowControlForAllTriggered, "the aggregate counter is separate from the queue one");
        Eq(0, all.FlowControlForQueueTriggered, "and it never borrowed the count/size counter");
        Assert.Contains("the consume request count exceeds threshold 1", allWarn.Warns[0]);
        Assert.Contains("cached=2", allWarn.Warns[0]);
        Assert.Contains("flowControlTimes=1001", allWarn.Warns[0]);

        // Span gate: the third counter, with one warn on trip 1.
        var span = Offline("CadenceSpan", mq);
        span.ConsumeMaxSpan = 10;
        Stage(span, new List<MessageExt> { Cached(mq, 0, 64), Cached(mq, 11, 64) });
        Captured spanWarn = CaptureGateLines(span, mq, 1);
        Eq(1, span.FlowControlForSpanTriggered, "one refusal on the span counter");
        Eq(0, span.FlowControlForQueueTriggered, "the span gate shares nothing with count/size");
        Assert.Contains("The queue's messages, span too long, so do flow control", spanWarn.Warns[0]);
        Assert.Contains("minOffset=0, maxOffset=11, maxSpan=11", spanWarn.Warns[0]);
        Assert.Contains("flowControlTimes=1", spanWarn.Warns[0]);
    }

    /// <summary>Every refusal logs the queue, the measured value, the threshold and the
    /// counter at debug, and its warn follows the contract's wording for the gate that
    /// fired.</summary>
    [Fact]
    public void RefusalLines_CarryQueueMeasuredThresholdAndCounter()
    {
        MessageQueue mq = OfflineQueue(0);

        // Count gate: the measured MiB travels with the count, and the queue is named.
        var count = Offline("LineCount", mq);
        count.PullThresholdForQueue = 2;
        Stage(count, Batch(mq, 3, MiB / 2, firstOffset: 5));
        Captured countLines = CaptureGateLines(count, mq, 1);
        Eq(1, count.FlowControlForQueueTriggered, "one refusal, exactly one count");
        string debug = Assert.Single(countLines.Debugs);
        Assert.Contains("count=3", debug);
        Assert.Contains("over pullThresholdForQueue=2", debug);
        Assert.Contains("flowControlTimes=1", debug);
        Assert.Contains("pause pull for 50 ms", debug);
        Assert.Contains(Broker, debug);
        Assert.Contains(Topic, debug);
        string countWarn = Assert.Single(countLines.Warns);
        Assert.Contains("The cached message count exceeds the threshold 2, so do flow control", countWarn);
        Assert.Contains("minOffset=5, maxOffset=7, count=3, size=1 MiB", countWarn);

        // Size gate: same shape, the byte threshold in MiB.
        var size = Offline("LineSize", mq);
        size.PullThresholdSizeForQueue = 1;
        Stage(size, Batch(mq, 2, 2 * MiB, firstOffset: 3));
        Captured sizeLines = CaptureGateLines(size, mq, 1);
        string sizeWarn = Assert.Single(sizeLines.Warns);
        Assert.Contains("The cached message size exceeds the threshold 1 MiB, so do flow control", sizeWarn);
        Assert.Contains("minOffset=3, maxOffset=4, count=2, size=4 MiB", sizeWarn);
        Assert.Contains(sizeLines.Debugs, l => l.Contains("over pullThresholdSizeForQueue=1"));

        // Aggregate gate: its debug line names the whole-buffer threshold.
        var all = Offline("LineAll", mq);
        all.PullThresholdForAll = 1;
        Stage(all, Batch(mq, 2, 64));
        Captured allLines = CaptureGateLines(all, mq, 1);
        string allDebug = Assert.Single(allLines.Debugs);
        Assert.Contains("cached=2 over pullThresholdForAll=1", allDebug);
        Assert.Contains("flowControlTimes=1", allDebug);
    }

    // ---------------------------------------------------------------- 9. the park is per queue

    /// <summary>A refusal re-schedules only its own queue: the armed deadline says 50 ms and
    /// the siblings of the same consumer record nothing.</summary>
    [Fact]
    public void Refusal_ParksOnlyItsOwnQueueForFiftyMillis()
    {
        MessageQueue hot = OfflineQueue(0);
        MessageQueue cold = OfflineQueue(3);
        var c = Offline("ParkOnlyHot", hot, cold);
        c.PullThresholdForQueue = 2;
        Stage(c, Batch(hot, 3, MiB / 2, firstOffset: 5));

        Assert.True(Hit(c, hot), "the hot queue's round is refused");
        Eq(50, c.LastPullPauseMillis(hot), "the pause the refusal recorded says 50 ms");
        Assert.True(c.PullParkRemainingMillis(hot) > 0, "and its deadline is armed in the future");
        Eq(DefaultLitePullConsumer.CacheFlowControlBackoffMillis, c.LastPullPauseMillis(hot),
            "the recorded pause is the shared cache-flow-control constant");
        Eq(DefaultLitePullConsumer.NoPullPause, c.LastPullPauseMillis(cold),
            "the cold sibling armed no pause at all: a refusal re-schedules only its own queue");
        Assert.False(Hit(c, cold), "and the sibling's own round is untouched");
    }

    // ---------------------------------------------------------------- 10. accumulators follow the buffer

    /// <summary>The per-queue view must describe exactly what the buffer holds: it grows on
    /// append, shrinks as the application polls, is re-derived by a seek, and a queue this
    /// instance no longer owns never refuses a pull from a stale accumulator.</summary>
    [Fact]
    public void Accumulators_TrackTheBuffer_ThroughAppendPollSeekAndUnassign()
    {
        MessageQueue q0 = OfflineQueue(0);
        MessageQueue q1 = OfflineQueue(1);
        var c = Offline("Accumulators", q0, q1);
        c.PullThresholdForQueue = 4;
        c.ConsumeMaxSpan = 50;

        Stage(c, new List<MessageExt>
        {
            Cached(q0, 10, 700), Cached(q0, 20, 700), Cached(q1, 30, 900),
        });
        Eq(3, c.CachedMessageCount, "the buffer total");
        Eq(2, c.CachedMessageCountOf(q0), "queue 0's own count");
        Eq(1, c.CachedMessageCountOf(q1), "queue 1's own count");
        Eq(1400, c.CachedBytesOf(q0), "queue 0's own stored bytes");
        Eq(10, c.CachedOffsetSpanOf(q0), "queue 0's own span");
        Eq(0, c.CachedOffsetSpanOf(q1), "one cached message spans 0");

        // A poll hands out the head of the FIFO (both queues interleaved) and the view
        // shrinks with it.
        List<MessageExt> polled = c.Poll(200);
        Eq(3, polled.Count, "the drain took everything that was cached");
        Eq(0, c.CachedMessageCount, "the buffer is dry");
        Eq(0, c.CachedMessageCountOf(q0), "and so is queue 0's view");
        Eq(0, c.CachedBytesOf(q0), "its bytes are gone too");
        Eq(0, c.CachedOffsetSpanOf(q0), "and its span reads 0, never a stale leftover");
        Assert.False(Hit(c, q0), "a drained buffer releases the gate -- the consumer is not stuck");

        // Refill past the count gate, then let a seek prune the head: the view is rebuilt
        // from what the buffer really kept, so the release is measured, not remembered.
        Stage(c, new List<MessageExt>
        {
            Cached(q0, 0, 500), Cached(q0, 60, 500), Cached(q0, 70, 500),
            Cached(q0, 80, 500), Cached(q0, 90, 500),
        });
        Assert.True(Hit(c, q0), "five cached messages over a budget of 4 refuses");
        Eq(90, c.CachedOffsetSpanOf(q0), "the span before the seek");
        c.Seek(q0, 70);
        Eq(3, c.CachedMessageCountOf(q0), "the seek pruned the buffer and the view with it");
        Eq(20, c.CachedOffsetSpanOf(q0), "the span is re-derived from the offsets that are left");
        Assert.False(Hit(c, q0), "and the round is allowed again");

        // A revoked queue cannot refuse a pull from an accumulator no queue owns, even while
        // its messages are still sitting in the buffer.
        Stage(c, new List<MessageExt> { Cached(q0, 200, 500), Cached(q0, 201, 500) });
        Assert.True(Hit(c, q0), "queue 0 owns the backlog again for this round");
        c.Assign(new[] { q1 });
        Assert.False(Hit(c, q0), "a revoked queue cannot refuse a pull -- nothing owns it");
        Eq(DefaultLitePullConsumer.NoPullPause, c.LastPullPauseMillis(q0),
            "and its park went away with the assignment");

        // While it is unowned its messages neither vanish nor drift: they are still cached,
        // so the whole-buffer budget counts them, and polling them away clears the view.
        var orphan = Offline("Orphaned", q1);
        orphan.PullThresholdForAll = 0;
        Stage(orphan, new List<MessageExt> { Cached(q1, 0, 64) });
        Assert.True(Hit(orphan, q1), "a cached message is part of the buffer the aggregate measures");
        while (orphan.CachedMessageCount > 0) orphan.Poll(50);
        Eq(0, orphan.CachedMessageCountOf(q1), "the view cleared with the buffer");
        Assert.False(Hit(orphan, q1), "once it is polled away the aggregate is empty again");
    }

    /// <summary>Nothing else moves on a refused round: no offset committed, the cursor where
    /// it was, and the messages still in the buffer.</summary>
    [Fact]
    public void Refusal_CommitsNothing_AndMovesNoCursor()
    {
        MessageQueue mq = OfflineQueue(0);
        var c = Offline("NoSideEffects", mq);
        c.PullThresholdForQueue = 1;
        Stage(c, Batch(mq, 2, 64, firstOffset: 40));
        long cursorBefore = c.PullCursorOf(mq);
        Assert.True(Hit(c, mq));
        Eq(cursorBefore, c.PullCursorOf(mq), "the pull cursor did not move");
        Eq(-1, c.ConsumeCursorOf(mq), "nothing was consumed");
        Eq(-1, c.PendingCommitOf(mq), "nothing was queued for commit");
        Eq(2, c.CachedMessageCount, "the backlog stayed exactly where it was");
    }

    // ---------------------------------------------------------------- 11. broker throttle 215, on the wire

    /// <summary>A lite pull answered FLOW_CONTROL(215) re-schedules this queue in 20 ms;
    /// any other broker failure keeps the port's existing failed-pull handling. Both legs in
    /// one test, so what is pinned is the difference between them.</summary>
    [Fact]
    public void BrokerFlowControl215_ParksTwentyMillis_WhileSystemBusyKeepsTheErrorBackoff()
    {
        // The decision itself, offline and timer-free.
        Eq(20, DefaultLitePullConsumer.LitePullPauseMillisForBrokerResponse(ResponseCode.FlowControl),
            "a broker throttle on this path is not an error: 20 ms");
        Eq(DefaultLitePullConsumer.BrokerFlowControlBackoffMillis,
            DefaultLitePullConsumer.LitePullPauseMillisForBrokerResponse(ResponseCode.FlowControl),
            "the same shared constant the push path uses");
        Eq(DefaultLitePullConsumer.NoPullPause,
            DefaultLitePullConsumer.LitePullPauseMillisForBrokerResponse(ResponseCode.SystemBusy),
            "every other code keeps the lite path's own failed-pull handling");

        // Leg 1: 215 on the wire records the 20 ms park, and the queue asks again.
        using (var cluster = MockCluster.Start(1))
        {
            cluster.ScriptPull(ResponseCode.FlowControl, null);
            DefaultLitePullConsumer c = StartedLite("Busy215", cluster);
            try
            {
                MessageQueue mq = WireQueue(0);
                Assert.True(WaitFor(() => c.LastPullPauseMillis(mq) == 20),
                    "the 215 answer re-scheduled this queue in 20 ms");
                Assert.True(WaitFor(() => LitePulls(cluster) >= 2),
                    "and the queue asked again instead of parking on the error backoff");
                Eq(20, c.LastPullPauseMillis(mq), "the recorded pause is the broker one, not the cache one");
                Eq(0, c.FlowControlForQueueTriggered,
                    "a broker throttle is not a cache refusal: none of the client's gate counters moved");
                Eq(0, c.PullCursorOf(mq), "the cursor is untouched by the 215 answer");
            }
            finally
            {
                c.Shutdown();
            }
        }

        // Leg 2 (control): SYSTEM_BUSY records no per-queue park -- the failed pull falls
        // back to whatever this path already did.
        using (var cluster = MockCluster.Start(1))
        {
            cluster.ScriptPull(ResponseCode.SystemBusy, null);
            DefaultLitePullConsumer c = StartedLite("BusyOther", cluster);
            try
            {
                MessageQueue mq = WireQueue(0);
                Assert.True(WaitFor(() => LitePulls(cluster) >= 2),
                    "a generic broker failure still retries on the regular interval");
                Eq(DefaultLitePullConsumer.NoPullPause, c.LastPullPauseMillis(mq),
                    "and armed no park: only 215 gets the short broker pause");
                Eq(0, c.CachedMessageCount, "and the refused answer cached nothing");
            }
            finally
            {
                c.Shutdown();
            }
        }
    }

    // ---------------------------------------------------------------- 12. shutdown and the round

    /// <summary>A queue parked on a cache gate still stops promptly when the consumer shuts
    /// down: the park is a deadline the round skips, not a wait shutdown has to cut short.</summary>
    [Fact]
    public void ParkedQueue_StopsPromptlyOnShutdown()
    {
        using var cluster = MockCluster.Start(1);
        MessageQueue mq = WireQueue(0);
        DefaultLitePullConsumer c = Lite("ShutdownEscape", cluster, mq);
        c.PullThresholdForQueue = 1;
        Stage(c, Batch(mq, 8, 64));
        try
        {
            c.Start();
            Assert.True(WaitFor(() => c.LastPullPauseMillis(mq) == 50),
                "the queue is parked on the gate before shutdown");

            var sw = Stopwatch.StartNew();
            c.Shutdown();
            sw.Stop();
            Assert.False(c.IsStarted);
            Assert.True(sw.ElapsedMilliseconds < 1500,
                $"shutting down while a queue is parked must not wait on the park (took "
                + $"{sw.ElapsedMilliseconds} ms)");

            // Parked, not emptied: the backlog is still there for the application.
            Eq(8, c.CachedMessageCount, "the refusal left the backlog in the buffer");
        }
        finally
        {
            c.Shutdown();
        }
    }

    /// <summary>A parked queue neither stalls its siblings' rounds nor the shutdown of the
    /// consumer that holds it.</summary>
    [Fact]
    public void ParkedQueue_DoesNotStallTheRoundsOfItsSiblings()
    {
        using var cluster = MockCluster.Start(1);
        MessageQueue hot = WireQueue(0);
        MessageQueue cold = WireQueue(1);
        DefaultLitePullConsumer c = Lite("NoStall", cluster, hot, cold);
        c.PullThresholdForQueue = 1;
        Stage(c, Batch(hot, 4, 64));
        try
        {
            c.Start();
            Assert.True(WaitFor(() => c.LastPullPauseMillis(hot) == 50), "the hot queue is parked");
            // The sibling keeps reaching the broker several times over one parked queue.
            Assert.True(WaitFor(() => LitePullsOf(cluster, 1) >= 3),
                "the round still visits the sibling while the first queue sits on its deadline");
            Eq(0, LitePullsOf(cluster, 0), "and the parked queue sent nothing of its own");
        }
        finally
        {
            c.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 13. found messages count towards the gate

    /// <summary>The gates read what the buffer really holds, including the messages a pull
    /// just handed it: a broker that keeps answering FOUND with a backlog over the budget is
    /// stopped by the gate, not by the application polling.</summary>
    [Fact]
    public void Gates_CountMessagesHandedToTheBufferByAPull_AndStopTheNextRound()
    {
        using var cluster = MockCluster.Start(1);
        MessageQueue mq = WireQueue(0);
        cluster.ScriptPull(ResponseCode.Success, new Dictionary<string, string>
        {
            ["nextBeginOffset"] = "4",
            ["minOffset"] = "0",
            ["maxOffset"] = "4",
        }, FoundBody(4, 0));
        DefaultLitePullConsumer c = Lite("FilledByPull", cluster, mq);
        c.PullThresholdForQueue = 3;
        try
        {
            c.Start();
            Assert.True(WaitFor(() => c.CachedMessageCount == 4),
                "the pull's own messages land in the buffer the gates measure");
            Assert.True(WaitFor(() => c.LastPullPauseMillis(mq) == 50),
                "and the very next round is refused on that backlog");
            int frozen = LitePulls(cluster);
            Assert.True(WaitRefusals(c, c.FlowControlForQueueTriggered, 5),
                "the gate went on refusing while the application never polled");
            Eq(frozen, LitePulls(cluster), "the client stops asking even though the application never polled");
            Eq(4, c.CachedMessageCountOf(mq), "counted per queue as well as in total");
            Eq(3, c.CachedOffsetSpanOf(mq), "and the span comes from the offsets that arrived");
        }
        finally
        {
            c.Shutdown();
        }
    }

    /// <summary>A pull body in the 17-field storage format, the same encoding path the
    /// cursor cases use.</summary>
    private static byte[] FoundBody(int count, long firstOffset)
    {
        using var ms = new MemoryStream();
        for (int i = 0; i < count; i++)
        {
            var m = new MessageExt
            {
                Topic = Topic,
                BrokerName = MockCluster.BrokerName(0),
                QueueId = 0,
                QueueOffset = firstOffset + i,
                SysFlag = 0,
                BornTimestamp = 1700000000000L,
                StoreTimestamp = 1700000001000L,
                BornHost = "127.0.0.1",
                BornHostPort = 10911,
                StoreHost = "127.0.0.1",
                StoreHostPort = 10911,
                CommitLogOffset = 4096 + 512 * i,
                Body = Encoding.UTF8.GetBytes("gate"),
            };
            byte[] frame = MessageDecoder.EncodeMessageExt(m, false);
            ms.Write(frame, 0, frame.Length);
        }

        return ms.ToArray();
    }
}
