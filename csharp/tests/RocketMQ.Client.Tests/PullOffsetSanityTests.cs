// Coverage for the push consumer's pull-result offset sanity warning.
//
// The contract: a FOUND round remembers the cursor it asked with, advances the cursor to
// the answer's nextBeginOffset, and at the END of that round -- after the cursor moved and
// after the batch was handed to the consume side -- compares the answer's cursor and the
// queue offset of the FIRST row it delivers against the remembered one. Either number
// sitting below the requested cursor means the round's own numbers contradict each other,
// and the round writes one WARN carrying all three. The warning is purely diagnostic: the
// stored cursor, the delivered rows and the cadence are exactly what an otherwise identical
// silent round leaves behind. A round that delivers no row reports long.MaxValue for the
// row offset, so the row leg can never fire for it.
//
// The pure legs pin the two comparisons and the empty-round sentinel. The cluster legs drive
// the real per-queue pull loop against the in-process fake broker and read the warning out of
// the log file the client itself wrote, together with the cursor every later round asked with
// (the stored cursor, read back off the wire). No leg races a timer.
using System.Globalization;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class PullOffsetSanityTests
{
    private const string Topic = "PullOffsetSanityTopic";
    private const string Group = "GID_PullOffsetSanity";
    private const string Broker = "broker-0";

    /// <summary>The subscription expression: rows tagged anything else are removed by
    /// the client-side second tag pass before delivery, which is exactly what makes a
    /// round's row list empty.</summary>
    private const string KeepTag = "kept";
    private const string DropTag = "dropped";

    /// <summary>The warning's own marker, so a leg reads the line this check wrote and
    /// not some other line the same window happens to log.</summary>
    private const string WarnMarker = "pull offset sanity warning";

    /// <summary>A pace value distinct from every wait the loop owns, used only to ask the
    /// cadence helper what the two compared rounds would wait.</summary>
    private const int Interval = 50;

    private static string StatsKey => Topic + "@" + Group;

    // ---------------------------------------------------------------------- pure legs

    /// <summary>Leg 1: the answer's cursor running below the cursor this round asked with
    /// fires -- and it fires on its own, with no row in the round at all.</summary>
    [Fact]
    public void OffsetsLookWrong_NextBeginOffsetBelowPrev_Fires()
    {
        Assert.True(DefaultMQPushConsumer.PullOffsetsLookWrong(
            nextBeginOffset: 5, firstMsgOffset: 10, prevRequestOffset: 10));
        // The cursor leg does not need a row to stand on: an empty round -- the
        // long.MaxValue sentinel -- still fires on a backwards cursor.
        Assert.True(DefaultMQPushConsumer.PullOffsetsLookWrong(
            nextBeginOffset: 5, firstMsgOffset: long.MaxValue, prevRequestOffset: 10));
    }

    /// <summary>Leg 2: the first delivered row sitting below the requested cursor fires on
    /// its own, while the very same cursors answer silent. Without the row leg these two
    /// calls would be one assertion, which is what makes the leg real.</summary>
    [Fact]
    public void OffsetsLookWrong_FirstMsgBelowPrev_WithConsistentCursor_Fires()
    {
        Assert.True(DefaultMQPushConsumer.PullOffsetsLookWrong(
            nextBeginOffset: 10, firstMsgOffset: 3, prevRequestOffset: 10));
        Assert.False(DefaultMQPushConsumer.PullOffsetsLookWrong(
            nextBeginOffset: 10, firstMsgOffset: 10, prevRequestOffset: 10));
    }

    /// <summary>Leg 3: a consistent round is silent, and both comparisons are strict --
    /// equality with the requested cursor is the normal case, not a breach.</summary>
    [Fact]
    public void OffsetsLookWrong_ConsistentRound_IsSilent_AndEqualityIsNotBreach()
    {
        Assert.False(DefaultMQPushConsumer.PullOffsetsLookWrong(
            nextBeginOffset: 10, firstMsgOffset: 10, prevRequestOffset: 10));
        Assert.False(DefaultMQPushConsumer.PullOffsetsLookWrong(
            nextBeginOffset: 11, firstMsgOffset: 10, prevRequestOffset: 10));
        Assert.False(DefaultMQPushConsumer.PullOffsetsLookWrong(
            nextBeginOffset: 10, firstMsgOffset: 11, prevRequestOffset: 10));
        // The whole queue at offset 0, asked at 0: the loop's very first round must
        // not open with a warning.
        Assert.False(DefaultMQPushConsumer.PullOffsetsLookWrong(
            nextBeginOffset: 0, firstMsgOffset: 0, prevRequestOffset: 0));
    }

    /// <summary>Leg 4: an empty row list has no row to point at, so it reports
    /// long.MaxValue -- the sentinel that keeps the row leg silent instead of reading a
    /// missing value as a backwards offset.</summary>
    [Fact]
    public void FirstMsgOffset_EmptyList_IsTheMaxValueSentinel()
    {
        Assert.Equal(long.MaxValue,
            DefaultMQPushConsumer.PullFirstMsgOffset(new List<MessageExt>()));
        // And with that sentinel the row leg cannot fire, whatever the cursor was.
        Assert.False(DefaultMQPushConsumer.PullOffsetsLookWrong(
            nextBeginOffset: 10, firstMsgOffset: long.MaxValue, prevRequestOffset: 10));
    }

    /// <summary>The row offset is the FIRST row of the list the round delivers, not the
    /// smallest one: a later row sitting below the cursor is not this criterion.</summary>
    [Fact]
    public void FirstMsgOffset_ReadsTheFirstRowNotTheSmallest()
    {
        Assert.Equal(10, DefaultMQPushConsumer.PullFirstMsgOffset(
            new List<MessageExt> { Row(10), Row(3) }));
        Assert.Equal(3, DefaultMQPushConsumer.PullFirstMsgOffset(
            new List<MessageExt> { Row(3), Row(10) }));
    }

    /// <summary>The quiet legs: only a FOUND round is checked at all. The other three
    /// statuses are answered by the same loop, and a backwards cursor inside an idle or
    /// corrected answer is not this warning's business -- pinning them here is what keeps
    /// the check from putting a line in the log on every idle queue.</summary>
    [Fact]
    public void OffsetSanity_AppliesToTheFoundRoundOnly()
    {
        Assert.True(DefaultMQPushConsumer.PullOffsetSanityApplies(PullStatus.Found));
        foreach (PullStatus status in new[]
        {
            PullStatus.NoNewMsg, PullStatus.NoMatchedMsg, PullStatus.OffsetIllegal,
        })
        {
            Assert.False(DefaultMQPushConsumer.PullOffsetSanityApplies(status));
        }
    }

    // ------------------------------------------------------------------ cluster legs

    /// <summary>End to end, leg 1: the round that was asked at 10 and answered 5 writes
    /// exactly one WARN, and the line carries all three numbers.</summary>
    [Fact]
    public void PullLoop_BackwardsCursor_WarnsWithAllThreeNumbers()
    {
        RoundOutcome run = RunFoundRound(parkedCursor: 10, answeredCursor: 5,
            rowOffsets: new[] { 10L }, rowTag: KeepTag, expression: KeepTag,
            expectedListenerRows: 1);

        string line = Assert.Single(run.WarnLines);
        Assert.Contains(" WARN  [", line);
        Assert.Contains("nextBeginOffset=5", line);
        Assert.Contains("firstMsgOffset=10", line);
        Assert.Contains("prevRequestOffset=10", line);
    }

    /// <summary>End to end, leg 2: the row leg fires on its own -- the cursor is perfectly
    /// consistent, the first delivered row is not.</summary>
    [Fact]
    public void PullLoop_BackwardsFirstRow_Warns_WhenTheCursorIsConsistent()
    {
        RoundOutcome run = RunFoundRound(parkedCursor: 10, answeredCursor: 10,
            rowOffsets: new[] { 3L }, rowTag: KeepTag, expression: KeepTag,
            expectedListenerRows: 1);

        string line = Assert.Single(run.WarnLines);
        Assert.Contains(" WARN  [", line);
        Assert.Contains("nextBeginOffset=10", line);
        Assert.Contains("firstMsgOffset=3", line);
        Assert.Contains("prevRequestOffset=10", line);
    }

    /// <summary>End to end, leg 3: a round whose cursor and rows both sit at or above the
    /// requested cursor writes no warning at all. Reverting the comparisons to an
    /// unconditional warn goes red here.</summary>
    [Fact]
    public void PullLoop_ConsistentFoundRound_WritesNoWarning()
    {
        RoundOutcome run = RunFoundRound(parkedCursor: 10, answeredCursor: 12,
            rowOffsets: new[] { 10L, 11L }, rowTag: KeepTag, expression: KeepTag,
            expectedListenerRows: 2);

        Assert.Empty(run.WarnLines);
        Assert.Equal(2, run.BookedTps);
    }

    /// <summary>End to end, leg 4: a FOUND round that carries no rows has no row to point
    /// at, so the row leg stays silent -- the warning it can still earn is the cursor's,
    /// and the line then shows the sentinel rather than a made-up offset.</summary>
    [Fact]
    public void PullLoop_EmptyFoundRound_RowLegStaysSilent_ButTheCursorLegStillFires()
    {
        RoundOutcome silent = RunFoundRound(parkedCursor: 10, answeredCursor: 10,
            rowOffsets: Array.Empty<long>(), rowTag: KeepTag, expression: KeepTag,
            expectedListenerRows: 0);
        Assert.Empty(silent.WarnLines);

        RoundOutcome firing = RunFoundRound(parkedCursor: 10, answeredCursor: 4,
            rowOffsets: Array.Empty<long>(), rowTag: KeepTag, expression: KeepTag,
            expectedListenerRows: 0);
        string line = Assert.Single(firing.WarnLines);
        Assert.Contains("firstMsgOffset=" + long.MaxValue.ToString(CultureInfo.InvariantCulture),
            line);
        Assert.Contains("nextBeginOffset=4", line);
        Assert.Contains("prevRequestOffset=10", line);
    }

    /// <summary>The row offset is taken AFTER this client's own pre-delivery filter: a
    /// round whose only row is removed by the second tag pass delivers nothing, so it is
    /// an empty round for this check -- and its rows really were dropped, which is what
    /// keeps the assertion from passing vacuously.</summary>
    [Fact]
    public void PullLoop_FoundRoundEmptiedByTheFilter_WritesNoWarning()
    {
        RoundOutcome run = RunFoundRound(parkedCursor: 10, answeredCursor: 10,
            rowOffsets: new[] { 3L }, rowTag: DropTag, expression: KeepTag,
            expectedListenerRows: 0);

        Assert.Equal(1, run.FilteredCount);
        Assert.Empty(run.WarnLines);
        // The round delivered nothing, so neither the TPS booking nor the listener has
        // anything: the row leg is silent because its input really is the sentinel.
        Assert.Equal(0, run.BookedTps);
        Assert.Equal(0, run.ListenerRows);
    }

    /// <summary>The quiet legs end to end: a NO_NEW_MSG and a NO_MATCHED_MSG answer with
    /// the same backwards cursor that makes a FOUND round warn write no line at all --
    /// the gate is the status, and it is the loop's own gate, not a property of the
    /// numbers.</summary>
    [Fact]
    public void PullLoop_QuietStatuses_WithABackwardsCursor_WriteNoWarning()
    {
        foreach (int code in new[]
        {
            ResponseCode.PullNotFound, ResponseCode.PullRetryImmediately,
        })
        {
            RoundOutcome run = RunRound(code, parkedCursor: 10, answeredCursor: 5,
                rowOffsets: Array.Empty<long>(), rowTag: KeepTag, expression: KeepTag,
                expectedListenerRows: 0);
            Assert.Empty(run.WarnLines);
            // The cursor still moved to the answer's value: only the line is suppressed.
            Assert.Equal("5", run.RequestedOffsets[2]);
        }
    }

    /// <summary>Leg 5: the check is diagnostic only. The firing round and an otherwise
    /// identical silent round differ in one number -- the first row's offset -- so they
    /// must store the same cursor, deliver the same rows, and hand the cadence helper the
    /// same input. The cursor is read back off the wire from the rounds that follow.</summary>
    [Fact]
    public void PullLoop_FiringRound_StoresSameCursorAndCadenceAsTheIdenticalSilentRound()
    {
        RoundOutcome firing = RunFoundRound(parkedCursor: 10, answeredCursor: 10,
            rowOffsets: new[] { 3L }, rowTag: KeepTag, expression: KeepTag,
            expectedListenerRows: 1);
        RoundOutcome silent = RunFoundRound(parkedCursor: 10, answeredCursor: 10,
            rowOffsets: new[] { 10L }, rowTag: KeepTag, expression: KeepTag,
            expectedListenerRows: 1);

        // The pair really differs in the verdict, and nothing else.
        Assert.Single(firing.WarnLines);
        Assert.Empty(silent.WarnLines);

        // Same stored cursor: every round after the one under test asks with the value
        // the round wrote, so an equal prefix means an equal write. The prefix also pins
        // the round's own input -- it was asked at 10, not at anything else. Only the
        // prefix is comparable: each run keeps polling at its own rate, so the tail
        // length proves nothing and asserting on it would make the leg flaky by
        // construction.
        Assert.Equal(new[] { "0", "10", "10" }, firing.RequestedOffsets.Take(3).ToList());
        Assert.Equal(silent.RequestedOffsets.Take(3).ToList(),
            firing.RequestedOffsets.Take(3).ToList());

        // Same rows handed over (PULL_TPS books the round's post-filter list
        // synchronously, and the listener is the same list one step later), so the
        // cadence helper is fed the same input and answers the same decision: the
        // interval still paces a real delivery on both legs.
        Assert.Equal(1, firing.BookedTps);
        Assert.Equal(1, silent.BookedTps);
        Assert.Equal(silent.ListenerRows, firing.ListenerRows);
        Assert.Equal(
            DefaultMQPushConsumer.PullNextRoundDelayMillis(PullStatus.Found,
                silent.BookedTps, Interval),
            DefaultMQPushConsumer.PullNextRoundDelayMillis(PullStatus.Found,
                firing.BookedTps, Interval));
        Assert.Equal(Interval,
            DefaultMQPushConsumer.PullNextRoundDelayMillis(PullStatus.Found,
                firing.BookedTps, Interval));
    }

    // ---------------------------------------------------------------------- harness

    private static MessageExt Row(long queueOffset) => new()
    {
        Topic = Topic,
        BrokerName = Broker,
        QueueId = 0,
        QueueOffset = queueOffset,
        Body = new byte[] { 1, 2, 3 },
    };

    private static MessageExt Row(long queueOffset, string tag)
    {
        MessageExt m = Row(queueOffset);
        m.Tags = tag;
        return m;
    }

    /// <summary>One FOUND answer's body: the rows back to back in the storage wire
    /// format, which is what the loop decodes into its round list.</summary>
    private static byte[] Rows(IReadOnlyList<long> queueOffsets, string tag)
    {
        using var ms = new MemoryStream();
        foreach (long off in queueOffsets)
        {
            byte[] frame = MessageDecoder.EncodeMessageExt(Row(off, tag), false);
            ms.Write(frame, 0, frame.Length);
        }

        return ms.ToArray();
    }

    private static Dictionary<string, string> PullResp(long next) => new()
    {
        ["nextBeginOffset"] = next.ToString(CultureInfo.InvariantCulture),
        ["minOffset"] = "0",
        ["maxOffset"] = next.ToString(CultureInfo.InvariantCulture),
    };

    /// <summary>A one-broker fake cluster whose route gives queues ONLY to
    /// <see cref="Topic"/>, so exactly one pull loop ever hits this broker and the
    /// scripted answers cannot be stolen by a sibling queue.</summary>
    private static MockCluster StartCluster()
    {
        var cluster = MockCluster.Start(1);
        cluster.SetRouteBody(req =>
        {
            string asked = req.ExtFields.TryGetValue("topic", out string? t) ? t : "";
            var route = new TopicRouteData();
            if (asked == Topic)
            {
                route.QueueDatas.Add(new QueueData(Broker, 1, 1,
                    PermName.PermRead | PermName.PermWrite, 0));
            }

            route.BrokerDatas.Add(new BrokerData("MockCluster", Broker,
                new SortedDictionary<long, string> { { MixAll.MasterId, cluster.BrokerAddrs[0] } }));
            return route.Encode();
        });
        return cluster;
    }

    /// <summary>Records every delivered row and succeeds immediately, so the consume side
    /// never holds a pull thread.</summary>
    private sealed class RecordingListener : IMessageListenerConcurrently
    {
        private readonly List<MessageExt> _seen = new();
        private readonly object _gate = new();

        public bool Orderly() => false;

        public List<MessageExt> Seen()
        {
            lock (_gate)
            {
                return new List<MessageExt>(_seen);
            }
        }

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
        {
            lock (_gate)
            {
                _seen.AddRange(msgs);
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    /// <summary>What one run left behind: the warning lines the client wrote, the cursor
    /// every pull request carried (the stored cursor read back off the wire), the row
    /// count PULL_TPS booked from the round's post-filter list, and the rows the filter
    /// removed.</summary>
    private sealed record RoundOutcome(
        List<string> WarnLines,
        List<string> RequestedOffsets,
        int BookedTps,
        long FilteredCount,
        int ListenerRows);

    /// <summary>Drives two scripted rounds through the real pull loop with the log file
    /// sink pointed at a temporary file, then returns the evidence.
    /// <list type="bullet">
    /// <item>round 1 is FOUND with no rows and parks the cursor at
    /// <paramref name="parkedCursor"/>, so round 2 asks with exactly that number;</item>
    /// <item>round 2 is the round under test: it answers
    /// <paramref name="secondCode"/> with <paramref name="answeredCursor"/> and carries
    /// the rows <paramref name="rowOffsets"/> tagged <paramref name="rowTag"/>, under the
    /// subscription expression <paramref name="expression"/>;</item>
    /// <item>every later round is unscripted and answers PULL_NOT_FOUND echoing the
    /// requested offset, so the offset on the wire IS the stored cursor.</item>
    /// </list>
    /// Waiting for the third request is what proves the round under test ran to its end --
    /// the warning is written inside it, before the pace wait that closes the round.</summary>
    private static RoundOutcome RunRound(int secondCode, long parkedCursor, long answeredCursor,
        long[] rowOffsets, string rowTag, string expression, int expectedListenerRows)
    {
        using MockCluster cluster = StartCluster();
        cluster.ScriptPull(ResponseCode.Success, PullResp(parkedCursor));
        cluster.ScriptPull(secondCode, PullResp(answeredCursor), Rows(rowOffsets, rowTag));

        string dir = Path.Combine(Path.GetTempPath(),
            "rmq_dotnet_sanity_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        string logPath = Path.Combine(dir, "sanity.log");
        // The capture must see every line this window writes: the rotation limits are
        // process-wide state, and a small limit left by another class would roll the
        // warning into a backup that gets deleted, so the warn would read as missing.
        long sizeBefore = ClientLog.LogFileMaxSize;
        int indexBefore = ClientLog.LogFileMaxIndex;
        var listener = new RecordingListener();
        DefaultMQPushConsumer c = NewConsumer(cluster, expression, listener);
        try
        {
            ClientLog.SetLogFileLimits(256L * 1024 * 1024, 1);
            ClientLog.SetLogFile(logPath);
            ClientLog.SetLogLevel(LogLevel.Debug);
            c.Start();
            Assert.True(WaitFor(() => RequestedOffsets(cluster).Count >= 3),
                "the round under test must run to its end and start the next one");
            if (expectedListenerRows > 0)
            {
                Assert.True(WaitFor(() => listener.Seen().Count >= expectedListenerRows),
                    "the delivered rows must reach the listener");
            }

            ClientLog.FlushLogFile();
            return new RoundOutcome(
                ReadAllLinesShared(logPath).Where(l => l.Contains(WarnMarker)).ToList(),
                RequestedOffsets(cluster),
                (int)Booked(c).value,
                c.FilteredMessageCount(),
                listener.Seen().Count);
        }
        finally
        {
            ClientLog.SetLogFile(string.Empty);
            ClientLog.FlushLogFile();
            ClientLog.SetLogLevel(LogLevel.Info);
            ClientLog.SetLogFileLimits(sizeBefore, indexBefore);
            QuickShutdown(c);
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

    /// <summary>The FOUND case of <see cref="RunRound"/>: the round under test answers
    /// SUCCESS, which is the only status this check looks at.</summary>
    private static RoundOutcome RunFoundRound(long parkedCursor, long answeredCursor,
        long[] rowOffsets, string rowTag, string expression, int expectedListenerRows) =>
        RunRound(ResponseCode.Success, parkedCursor, answeredCursor, rowOffsets, rowTag,
            expression, expectedListenerRows);

    private static DefaultMQPushConsumer NewConsumer(MockCluster cluster,
        string expression, IMessageListenerConcurrently listener)
    {
        var c = new DefaultMQPushConsumer(Group)
        {
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.InstanceName = "offset_sanity_" + Guid.NewGuid().ToString("N")[..8];
        c.Subscribe(Topic, expression);
        c.SetMessageListener(listener);
        // The clustering allocation reads the group's consumer list from the broker:
        // answer with this instance only, so it owns the queue.
        cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(new[] { c.ClientId }));
        return c;
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

    /// <summary>The queueOffset each pull request carried, in arrival order.</summary>
    private static List<string> RequestedOffsets(MockCluster cluster) =>
        cluster.Records()
            .Where(r => r.Code == RequestCode.PullMessage)
            .Select(r => r.Ext.TryGetValue("queueOffset", out string? v) ? v : "?")
            .ToList();

    /// <summary>PULL_TPS for this topic and group: the round books the post-filter list
    /// synchronously, so its value is the count the cadence decision was keyed on.</summary>
    private static (long value, long times) Booked(DefaultMQPushConsumer c)
    {
        StatsItem? item = c.Client().ConsumerStats.TopicAndGroupPullTPS.Find(StatsKey);
        return item is null ? (0L, 0L) : item.Snapshot();
    }

    // ClientLog holds the write handle, and the log is still being appended to by the pull
    // thread's own clock: read it shared for ReadWrite, the way `tail -f` does it.
    private static string[] ReadAllLinesShared(string path)
    {
        using FileStream fs = new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using StreamReader sr = new(fs);
        var lines = new List<string>();
        string? line;
        while ((line = sr.ReadLine()) is not null) lines.Add(line);
        return lines.ToArray();
    }

    private static bool WaitFor(Func<bool> cond, int millis = 15000)
    {
        long deadline = Environment.TickCount64 + millis;
        while (Environment.TickCount64 < deadline)
        {
            if (cond()) return true;
            Thread.Sleep(20);
        }

        return cond();
    }

    /// <summary>Shutdown without paying one whole rebalance period: the rebalance thread
    /// blocks in a long wait that the stop flags do not wake.</summary>
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
}
