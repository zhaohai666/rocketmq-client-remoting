// Unit tests for the pre-pull flow-control decision -- no cluster needed.
//
// Three properties of the gate are pinned here and each of them is a place a plausible
// implementation goes wrong:
//   * what is measured is the queue's HELD SET (the pulled-but-undispatched buffer union the
//     batches already in the listener's hands, deduplicated by queue offset), not the buffer;
//   * the byte term sums message BODIES and divides by one MiB with truncating integer
//     arithmetic, never the broker-side stored-record size;
//   * every gate is a plain strict `>` over the knob as it stands -- no floor that rounds a
//     threshold up, no value that silently switches a gate off (the legal range of each knob
//     is enforced once, at start-up).
// Since no gate has an off switch, a case isolates its own gate by putting the OTHER knobs
// out of reach, never by zeroing them.
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class FlowControlTests
{
    private const string Topic = "FlowControlNetUnitTopic";
    private const string OtherTopic = "FlowControlNetUnitOther";
    private const string Broker = "broker-a";
    private const int MiB = 1024 * 1024;

    private static MessageQueue QueueOf(string topic, int id) => new(topic, Broker, id);

    /// <summary>A held row as the queue buffer keeps it. The body is what the byte gate sums;
    /// the stored-record size is a telemetry number that defaults to a decoy small enough to
    /// never be mistaken for the measurement.</summary>
    private static MessageExt Row(string topic, long queueOffset, int bodyBytes, int storeSize = 64) => new()
    {
        Topic = topic,
        BrokerName = Broker,
        QueueId = 0,
        QueueOffset = queueOffset,
        StoreSize = storeSize,
        Body = new byte[bodyBytes],
    };

    /// <summary>A backlog of <c>n</c> consecutive offsets. The offsets have to differ: the held
    /// set is deduplicated by queue offset, so rows sharing one offset would count once.</summary>
    private static List<MessageExt> Rows(string topic, int n, int bodyBytes, long firstOffset = 0,
                                         int storeSize = 64)
        => Enumerable.Range(0, n).Select(i => Row(topic, firstOffset + i, bodyBytes, storeSize)).ToList();

    /// <summary>Put every gate but the message count out of reach, and set the count budget.
    /// Zeroing the other two would not isolate anything -- a 0 budget trips on the first row.</summary>
    private static void CountGateOnly(DefaultMQPushConsumer c, int budget)
    {
        c.PullThresholdForQueue = budget;
        c.PullThresholdSizeForQueue = int.MaxValue;
        c.ConsumeConcurrentlyMaxSpan = long.MaxValue;
        c.PullThresholdForTopic = -1;
        c.PullThresholdSizeForTopic = -1;
    }

    /// <summary>Same, for the byte budget.</summary>
    private static void SizeGateOnly(DefaultMQPushConsumer c, int budgetMib)
    {
        c.PullThresholdForQueue = int.MaxValue;
        c.PullThresholdSizeForQueue = budgetMib;
        c.ConsumeConcurrentlyMaxSpan = long.MaxValue;
        c.PullThresholdForTopic = -1;
        c.PullThresholdSizeForTopic = -1;
    }

    /// <summary>Same, for the offset span.</summary>
    private static void SpanGateOnly(DefaultMQPushConsumer c, long budget)
    {
        c.PullThresholdForQueue = int.MaxValue;
        c.PullThresholdSizeForQueue = int.MaxValue;
        c.ConsumeConcurrentlyMaxSpan = budget;
        c.PullThresholdForTopic = -1;
        c.PullThresholdSizeForTopic = -1;
    }

    /// <summary>Preloads a batch of messages into some queue's buffer and registers that
    /// queue as owned (the owned-queue table is the divisor of the topic-level budgets, see
    /// TopicThresholdTests). The queue is staged buffer-only: nothing is left in the
    /// listener's hands unless <see cref="StageHeld"/> put it there.</summary>
    private static void Stage(DefaultMQPushConsumer c, MessageQueue mq, List<MessageExt> msgs)
    {
        string key = DefaultMQPushConsumer.OffsetKeyForTest(mq);
        c.SetAssignedForTest(key, mq);
        c.SetInFlightForTest(key, null);
        c.SetPendingForTest(key, msgs);
    }

    /// <summary>Stage the same backlog plus a batch already handed to the listener: the lower
    /// offsets are the ones that were dispatched, so the two halves are disjoint.</summary>
    private static void StageHeld(DefaultMQPushConsumer c, MessageQueue mq,
                                  List<MessageExt> dispatched, List<MessageExt> buffered)
    {
        string key = DefaultMQPushConsumer.OffsetKeyForTest(mq);
        c.SetAssignedForTest(key, mq);
        c.SetInFlightForTest(key, dispatched);
        c.SetPendingForTest(key, buffered);
    }

    private static bool Hit(DefaultMQPushConsumer c, MessageQueue mq)
        => c.FlowControlHitForTest(mq, DefaultMQPushConsumer.OffsetKeyForTest(mq));

    // ClientLog keeps the write handle for the whole run, and Windows validates share modes in both
    // directions: the file must be opened shared for ReadWrite, the way `tail -f` does it.
    private static string[] ReadAllLinesShared(string path)
    {
        using FileStream fs = new(path, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
        using StreamReader sr = new(fs);
        var lines = new List<string>();
        string? line;
        while ((line = sr.ReadLine()) is not null) lines.Add(line);
        return lines.ToArray();
    }

    [Fact]
    public void Defaults_OnlyTheCountGateCanEverMatch()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlDefaultsNet");
        var mq = QueueOf(Topic, 0);
        Assert.Equal(1000, c.PullThresholdForQueue);
        Assert.Equal(100, c.PullThresholdSizeForQueue);
        Assert.Equal(2000L, c.ConsumeConcurrentlyMaxSpan);
        Assert.Equal(-1, c.PullThresholdForTopic);
        Assert.Equal(-1, c.PullThresholdSizeForTopic);
        // A small buffer is nowhere near any of the three budgets.
        Stage(c, mq, Rows(Topic, 1, 100));
        Assert.False(Hit(c, mq));
        Assert.Equal(0L, c.FlowControlTriggered);
        // An empty buffer (nothing pulled yet) does not match either, and the span must not come out negative
        Stage(c, mq, new List<MessageExt>());
        Assert.False(Hit(c, mq));

        // The knobs are stored exactly as written: nothing is rounded up on the way in.
        c.PullThresholdForQueue = 0;
        Assert.Equal(0, c.PullThresholdForQueue);
        c.PullThresholdForQueue = 7;
        Assert.Equal(7, c.PullThresholdForQueue);
        // The byte term divides by one MiB, and that modulus is the gate's own constant.
        Assert.Equal(1024L * 1024, DefaultMQPushConsumer.CacheSizeGateMib);
    }

    [Fact]
    public void CountGate_IsStrictlyGreater_AndHasNoFloor()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlCountNet");
        var mq = QueueOf(Topic, 0);
        CountGateOnly(c, 3);
        Stage(c, mq, Rows(Topic, 2, 100));
        Assert.False(Hit(c, mq));
        // The gate is strictly greater than the threshold: a queue that has reached 3 exactly is
        // still allowed to pull, so the threshold reads as "how many may be held".
        Stage(c, mq, Rows(Topic, 3, 100));
        Assert.False(Hit(c, mq), "exactly 3 rows against a threshold of 3 must not fire (>, not >=)");
        Assert.Equal(0L, c.FlowControlTriggered);
        Stage(c, mq, Rows(Topic, 4, 100));
        Assert.True(Hit(c, mq));
        Assert.Equal(1L, c.FlowControlTriggered);

        // No floor and no off switch: the gate reads a threshold of 0 as 0, so the FIRST held row
        // already trips it. That is precisely why a 0 is refused at start-up (CheckConfigRanges)
        // rather than being a usable setting -- an implementation that quietly counted max(1, n)
        // would let one row through here and would also keep working after the check was changed.
        CountGateOnly(c, 0);
        Stage(c, mq, new List<MessageExt>());
        Assert.False(Hit(c, mq), "an empty queue holds nothing, so even a 0 budget has nothing to trip on");
        Stage(c, mq, Rows(Topic, 1, 100));
        Assert.True(Hit(c, mq), "a threshold of 0 must count as 0 rows, not as 1");
    }

    [Fact]
    public void HeldSet_IsBufferUnionListenerRows_DedupedByQueueOffset()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlHeldNet");
        var mq = QueueOf(Topic, 0);
        // A batch handed to the listener has only moved: it still occupies this queue's budget, so
        // judging the buffer alone would keep pulling while the listener sits on messages.
        CountGateOnly(c, 4);
        var dispatched = Rows(Topic, 3, 100, firstOffset: 0);
        var buffered = Rows(Topic, 2, 100, firstOffset: 3);
        StageHeld(c, mq, dispatched, buffered);
        Assert.Equal(5, c.ProcessQueueEntriesForTest(DefaultMQPushConsumer.OffsetKeyForTest(mq)).Count);
        Assert.True(Hit(c, mq), "3 rows in the listener's hands + 2 still buffered = 5 held, over a gate of 4");
        // The buffer alone would read 2, and 2 is not over 4: this is the leg that fails for a
        // gate that forgets the dispatched half.
        Stage(c, mq, buffered);
        Assert.False(Hit(c, mq), "with the dispatched rows gone the same buffer is inside the budget");

        // And a row that is in both views is ONE row: the dispatch hands out the lower offsets and
        // the sweep keeps the buffer's copy until it settles.
        var overlap = Rows(Topic, 2, 100, firstOffset: 0);
        StageHeld(c, mq, overlap, overlap);
        Assert.Equal(2, c.ProcessQueueEntriesForTest(DefaultMQPushConsumer.OffsetKeyForTest(mq)).Count);
        Assert.False(Hit(c, mq), "the same 2 offsets handed out and still buffered are not 4 rows");

        // Dedup is by offset, not by object identity: different rows, same offsets.
        var again = Rows(Topic, 2, 100, firstOffset: 0);
        StageHeld(c, mq, overlap, again);
        Assert.Equal(2, c.ProcessQueueEntriesForTest(DefaultMQPushConsumer.OffsetKeyForTest(mq)).Count);

        // Settling releases the budget: the sweep removes exactly the entry it sent back.
        string key = DefaultMQPushConsumer.OffsetKeyForTest(mq);
        c.SetInFlightForTest(key, null);
        c.SetPendingForTest(key, null);
        Assert.False(Hit(c, mq));
    }

    [Fact]
    public void SizeGate_CountsBodyBytes_AndNotTheStoredRecord()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlSizeNet");
        var mq = QueueOf(Topic, 0);
        SizeGateOnly(c, 1);

        // The bytes this client actually keeps in memory are the bodies, so a large stored record
        // with a small body stays inside the budget...
        Stage(c, mq, new List<MessageExt> { Row(Topic, 0, bodyBytes: 4, storeSize: 2 * MiB) });
        Assert.False(Hit(c, mq), "4 bytes of body inside a 2MiB store record is 0 MiB held");

        // ... and a small stored record with a large body does not.
        Stage(c, mq, new List<MessageExt> { Row(Topic, 0, bodyBytes: 2 * MiB, storeSize: 4) });
        Assert.True(Hit(c, mq), "2MiB of body is 2 MiB held however small the store record is");

        // The summation is over every held row, buffer and listener alike.
        var c2 = new DefaultMQPushConsumer("GID_FlowControlSizeSumNet");
        SizeGateOnly(c2, 1);
        StageHeld(c2, mq, Rows(Topic, 2, 700 * 1024, firstOffset: 0),
                  Rows(Topic, 1, 700 * 1024, firstOffset: 2));
        Assert.True(Hit(c2, mq), "3 x 0.7MiB of body is 2.1MiB, which truncates to 2 MiB held");
    }

    [Fact]
    public void SizeGate_DividesByWholeMib_Truncating()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlMibNet");
        var mq = QueueOf(Topic, 0);
        SizeGateOnly(c, 1);
        Stage(c, mq, Rows(Topic, 3, 300));
        Assert.False(Hit(c, mq), "900 bytes is 0 whole MiB");
        Stage(c, mq, new List<MessageExt> { Row(Topic, 0, MiB) });
        Assert.False(Hit(c, mq), "exactly 1MiB against a 1MiB threshold must not fire (>, not >=)");
        // A partially filled MiB is not charged at all: 1 MiB + 1 byte still reads as 1 MiB.
        Stage(c, mq, new List<MessageExt> { Row(Topic, 0, MiB + 1) });
        Assert.False(Hit(c, mq), "the division truncates, so 1MiB+1 byte is still 1 MiB");
        // Which is what makes a 1 MiB budget tolerate a 1.9 MiB backlog, and refuse at 2 MiB.
        Stage(c, mq, new List<MessageExt> { Row(Topic, 0, (19 * MiB) / 10) });
        Assert.False(Hit(c, mq), "1.9MiB truncates to 1 and a 1MiB budget keeps pulling");
        Stage(c, mq, new List<MessageExt> { Row(Topic, 0, 2 * MiB) });
        Assert.True(Hit(c, mq), "2 whole MiB is strictly over a 1MiB budget");

        // No off switch either: with a budget of 0 the first WHOLE MiB trips it, while a sub-MiB
        // backlog still reads as 0 MiB and pulls.
        SizeGateOnly(c, 0);
        Stage(c, mq, new List<MessageExt> { Row(Topic, 0, MiB - 1) });
        Assert.False(Hit(c, mq), "0.9MiB is 0 whole MiB, and 0 is not over 0");
        Stage(c, mq, new List<MessageExt> { Row(Topic, 0, MiB) });
        Assert.True(Hit(c, mq), "a threshold of 0 must count as 0 MiB, not as 1");
    }

    [Fact]
    public void SpanGate_IsStrictlyGreater_AndMeasuresRealSpan()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlSpanNet");
        var mq = QueueOf(Topic, 0);
        SpanGateOnly(c, 10);
        Stage(c, mq, new List<MessageExt> { Row(Topic, 0, 1), Row(Topic, 100, 1) });
        Assert.True(Hit(c, mq));
        SpanGateOnly(c, 100);
        Assert.False(Hit(c, mq), "a span of 100 is not strictly greater than 100, it must not match");
        // an out-of-order buffer must still measure the real span (min/max, not first-minus-last)
        SpanGateOnly(c, 10);
        Stage(c, mq, new List<MessageExt>
        {
            Row(Topic, 50, 1), Row(Topic, 5, 1), Row(Topic, 7, 1),
        });
        Assert.True(Hit(c, mq), "an out-of-order backlog must measure the span as max-min, not first-minus-last");
        // One held row spans 0, so it is inside any budget, down to a budget of 0.
        SpanGateOnly(c, 0);
        Stage(c, mq, new List<MessageExt> { Row(Topic, 5000, 1) });
        Assert.False(Hit(c, mq), "a single row spans 0, which is not over a budget of 0");
        Stage(c, mq, new List<MessageExt> { Row(Topic, 5000, 1), Row(Topic, 5001, 1) });
        Assert.True(Hit(c, mq), "a budget of 0 is not switched off: a span of 1 trips it");

        // The span is this queue's own: rows the listener is holding count, and a sibling's
        // window never widens it.
        var hot = QueueOf(Topic, 0);
        var wild = QueueOf(Topic, 1);
        var c3 = new DefaultMQPushConsumer("GID_FlowControlSpanSiblingNet");
        SpanGateOnly(c3, 10);
        StageHeld(c3, hot, new List<MessageExt> { Row(Topic, 0, 1) },
                  new List<MessageExt> { Row(Topic, 4, 1) });
        Stage(c3, wild, new List<MessageExt> { Row(Topic, 0, 1), Row(Topic, 1000, 1) });
        Assert.False(Hit(c3, hot), "a sibling queue's offset window must not stop this queue");
        Assert.True(Hit(c3, wild));
    }

    /// <summary>The span knob is named after concurrent consumption and an orderly queue has no
    /// run-ahead to guard (its cursor only moves once the batch in the listener's hands settles),
    /// so the span term must not run for an orderly listener -- while the other two still do.</summary>
    private sealed class OrderlyGateListener : IMessageListenerOrderly
    {
        public bool Orderly() => true;

        public ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context)
            => ConsumeOrderlyStatus.Success;
    }

    [Fact]
    public void SpanGate_NeverJudgesAnOrderlyListener_ButTheOtherTwoStillDo()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlOrderlyNet");
        c.SetMessageListener(new OrderlyGateListener());
        var mq = QueueOf(Topic, 0);
        SpanGateOnly(c, 0);
        Stage(c, mq, new List<MessageExt> { Row(Topic, 0, 1), Row(Topic, 100000, 1) });
        Assert.False(Hit(c, mq), "an orderly queue is never span-gated");
        Assert.Equal(0L, c.FlowControlTriggered);

        // Control: the same consumer with the same backlog does judge its count budget.
        c.PullThresholdForQueue = 1;
        Assert.True(Hit(c, mq), "the count gate still judges an orderly queue's own backlog");

        // And a concurrent listener with the identical span does get span-gated.
        var concurrent = new DefaultMQPushConsumer("GID_FlowControlConcurrentSpanNet");
        SpanGateOnly(concurrent, 0);
        Stage(concurrent, mq, new List<MessageExt> { Row(Topic, 0, 1), Row(Topic, 100000, 1) });
        Assert.True(Hit(concurrent, mq), "the span term is the orderly path's only exemption");
    }

    [Fact]
    public void CacheBackoff_IsFiftyMillis_AndNotTheBrokersTwenty()
    {
        // The client's own cache threshold parks the pull for one short re-schedule period; the
        // broker's FLOW_CONTROL(215) reply is a different event with a different number.
        Assert.Equal(50, DefaultMQPushConsumer.CacheFlowControlBackoffMillis);
        Assert.NotEqual(DefaultMQPushConsumer.BrokerFlowControlBackoffMillis,
            DefaultMQPushConsumer.CacheFlowControlBackoffMillis);
    }

    /// <summary>The order the terms are judged in, and that a refusal ticks exactly one counter
    /// even when the same backlog is over every budget: count, then bytes, then span.
    /// The reasons are read off the debug channel, which carries EVERY trip -- the warn line only
    /// answers trips 1, 1001, 2001 and would say nothing about the second and third refusal.</summary>
    [Fact]
    public void Gates_AreJudgedCountThenSizeThenSpan_AndShareOneCounter()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlOrderNet");
        var mq = QueueOf(Topic, 0);
        c.PullThresholdForQueue = 1;
        c.PullThresholdSizeForQueue = 0;
        c.ConsumeConcurrentlyMaxSpan = 0;
        c.PullThresholdForTopic = -1;
        c.PullThresholdSizeForTopic = -1;
        Stage(c, mq, new List<MessageExt> { Row(Topic, 0, 2 * MiB), Row(Topic, 100, 2 * MiB) });

        List<string> reasons = CaptureGateReasons(c, mq, 1);
        Assert.Single(reasons);
        Assert.Contains("count=2", reasons[0]);
        Assert.DoesNotContain("size=", reasons[0]);
        Assert.DoesNotContain("span=", reasons[0]);

        // With the count budget out of reach the same backlog is judged by the byte term.
        c.PullThresholdForQueue = int.MaxValue;
        reasons = CaptureGateReasons(c, mq, 1);
        Assert.Contains("size=4MB", reasons[0]);
        Assert.DoesNotContain("span=", reasons[0]);

        // And with the byte budget out of reach too, by the span.
        c.PullThresholdSizeForQueue = int.MaxValue;
        reasons = CaptureGateReasons(c, mq, 1);
        Assert.Contains("span=100", reasons[0]);
        // three refusals, one client-wide counter
        Assert.Equal(3L, c.FlowControlTriggered);
    }

    /// <summary>Run <paramref name="rounds"/> verdicts with the log pointed at a private file and
    /// hand back the per-trip DEBUG lines the gate wrote.</summary>
    private static List<string> CaptureGateReasons(DefaultMQPushConsumer c, MessageQueue mq, int rounds)
    {
        List<string> all = CaptureGateLines(c, mq, rounds);
        // The writer pads the level name to five characters, so DEBUG is followed by exactly one
        // space before the pid bracket while WARN gets two.
        return all.Where(l => l.Contains(" DEBUG [") && l.Contains("flow control: queue")).ToList();
    }

    private static List<string> CaptureGateLines(DefaultMQPushConsumer c, MessageQueue mq, int rounds)
    {
        string dir = Path.Combine(Path.GetTempPath(),
            "rmq_dotnet_fc_" + Guid.NewGuid().ToString("N")[..8]);
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
                Assert.True(Hit(c, mq), $"round {i + 1} must pause the pull");
            }

            ClientLog.FlushLogFile();
            return ReadAllLinesShared(path).ToList();
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

    /// <summary>Warn trips counted from the log file written by the gate itself.</summary>
    private static List<string> CaptureWarnTrips(DefaultMQPushConsumer c, MessageQueue mq, int rounds)
    {
        string dir = Path.Combine(Path.GetTempPath(),
            "rmq_dotnet_fc_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        string path = Path.Combine(dir, "flow.log");
        // The capture has to see every line this window writes: the rotation limits are process-wide
        // state, and a small limit left by another class would roll the early trips into backups that
        // get deleted, so the warn would read as missing. Raised here, restored below.
        long sizeBefore = ClientLog.LogFileMaxSize;
        int indexBefore = ClientLog.LogFileMaxIndex;
        try
        {
            ClientLog.SetLogFileLimits(256L * 1024 * 1024, 1);
            ClientLog.SetLogFile(path);
            ClientLog.SetLogLevel(LogLevel.Debug);
            for (int i = 0; i < rounds; i++)
            {
                Assert.True(Hit(c, mq), $"round {i + 1} must pause the pull");
            }

            ClientLog.FlushLogFile();
            string[] lines = ReadAllLinesShared(path);
            return lines.Where(l => l.Contains(" WARN  [")).ToList();
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

    [Fact]
    public void WarnCadence_IsTheFirstTripAndEveryThousandthAfter()
    {
        foreach (long n in new[] { 1L, 1001L, 2001L })
        {
            Assert.True(DefaultMQPushConsumer.IsFlowControlWarnTrip(n), $"trip {n} must warn");
        }
        // Control: the trips in between stay on the debug channel only. A cadence degraded to
        // "warn always" or "warn once and stop" is caught here, not by the boundaries above.
        foreach (long n in new[] { 0L, 2L, 999L, 1000L, 1002L, 2000L })
        {
            Assert.False(DefaultMQPushConsumer.IsFlowControlWarnTrip(n), $"trip {n} must stay quiet");
        }
        Assert.Equal(1000, DefaultMQPushConsumer.FlowControlWarnEvery);

        var c = new DefaultMQPushConsumer("GID_FlowControlWarnNet");
        var mq = QueueOf(Topic, 0);
        CountGateOnly(c, 1);
        Stage(c, mq, Rows(Topic, 2, 100));

        List<string> warns = CaptureWarnTrips(c, mq, 1000);
        Assert.Equal(1000L, c.FlowControlTriggered);
        Assert.Single(warns);
        Assert.Contains("(flowControlTimes=1)", warns[0]);

        // The 1001st trip is the next warn, and it carries the counter it fired on.
        warns = CaptureWarnTrips(c, mq, 1);
        Assert.Equal(1001L, c.FlowControlTriggered);
        Assert.Single(warns);
        Assert.Contains("(flowControlTimes=1001)", warns[0]);
    }

    [Fact]
    public void WarnCapture_SurvivesASmallGlobalRotationLimit()
    {
        // The cadence above reads its warns back from the log file, and the rotation limits are
        // process-wide state: a small limit left behind by another class rolls the early trips into
        // backups that then get deleted, which looks exactly like "the warn never happened". The
        // capture raises the limits for its own window and puts them back, so nothing else can
        // break it -- and this test is the control for a capture that forgets to.
        long sizeBefore = ClientLog.LogFileMaxSize;
        int indexBefore = ClientLog.LogFileMaxIndex;
        try
        {
            ClientLog.SetLogFileLimits(2000, 3);

            var c = new DefaultMQPushConsumer("GID_FlowControlWarnLeakNet");
            var mq = QueueOf(Topic, 0);
            CountGateOnly(c, 1);
            Stage(c, mq, Rows(Topic, 2, 100));

            List<string> warns = CaptureWarnTrips(c, mq, 10);
            Assert.Equal(10L, c.FlowControlTriggered);
            Assert.Single(warns);
            Assert.Contains("(flowControlTimes=1)", warns[0]);

            // Restored, not raised-and-abandoned: the next class must find what this one found.
            Assert.Equal(2000L, ClientLog.LogFileMaxSize);
            Assert.Equal(3, ClientLog.LogFileMaxIndex);
        }
        finally
        {
            ClientLog.SetLogFileLimits(sizeBefore, indexBefore);
        }
    }

    /// <summary>The pull gate judges ONE queue's own backlog: a topic-level budget reaches it
    /// only as the divided per-queue number (see TopicThresholdTests), never by adding the
    /// sibling queues up. Four queues of a 1000-message topic budget therefore sit at 250
    /// each -- 1000 messages cached for this topic on this instance -- and not one queue
    /// pauses. Reverting to the pull-time aggregation reddens the first leg (topic total
    /// 1000 >= 1000 stops all four).</summary>
    [Fact]
    public void PullGate_NeverSumsSiblings_OnlyTheDividedSharePauses()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlNoSumNet");
        MessageQueue q0 = QueueOf(Topic, 0);
        MessageQueue q1 = QueueOf(Topic, 1);
        MessageQueue q2 = QueueOf(Topic, 2);
        MessageQueue q3 = QueueOf(Topic, 3);
        CountGateOnly(c, 1000);
        c.PullThresholdForTopic = 1000;
        MessageQueue[] all = { q0, q1, q2, q3 };
        foreach (MessageQueue mq in all)
        {
            Stage(c, mq, Rows(Topic, 250, 100));
        }

        // The division over the 4 owned queues is what changes, not the pull-time total.
        c.DivideTopicThresholdsForTest();
        Assert.Equal(250, c.PullThresholdForQueue);
        foreach (MessageQueue mq in all)
        {
            Assert.False(Hit(c, mq), $"{mq} sits at its own share of 250: a sibling's"
                                     + " backlog must not pause it");
        }

        Assert.Equal(0L, c.FlowControlTriggered);

        // One queue over its own share pauses, and it pauses on the COUNT reason of its own
        // backlog -- not on a topic total; its three siblings stay unpaused.
        Stage(c, q0, Rows(Topic, 251, 100));
        List<string> warns = CaptureWarnTrips(c, q0, 1);
        Assert.Equal(1L, c.FlowControlTriggered);
        Assert.Single(warns);
        Assert.Contains("count=251", warns[0]);
        Assert.DoesNotContain("topicCount", warns[0]);
        foreach (MessageQueue mq in new[] { q1, q2, q3 })
        {
            Assert.False(Hit(c, mq), $"{mq} is at 250 with its sibling over its share");
        }

        // A queue of ANOTHER topic is not a sibling at all: the count gate is per queue and
        // the topic knobs are not read here.
        MessageQueue other = QueueOf(OtherTopic, 0);
        Stage(c, other, Rows(OtherTopic, 250, 100));
        Assert.False(Hit(c, other));
    }

    /// <summary>Same contract for the byte budget: the divided gate judges this queue's own
    /// bytes, so a topic-wide total over the budget is still inside the limit as long as no
    /// single queue is. With the old pull-time topic sum these two half-MiB buffers (1MiB
    /// across the topic against a 1MiB-per-queue share) would both pause.</summary>
    [Fact]
    public void SizeGate_JudgesOwnBytes_AndNotTheSiblingsTotal()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlNoSumSizeNet");
        MessageQueue q0 = QueueOf(Topic, 0);
        MessageQueue q1 = QueueOf(Topic, 1);
        SizeGateOnly(c, 100);
        // 2 MiB for the topic over the 2 queues this consumer owns -> 1 MiB per queue, and
        // the queue-level switch is only ever written by the division.
        c.PullThresholdSizeForTopic = 2;
        Stage(c, q0, new List<MessageExt> { Row(Topic, 0, MiB / 2) });
        Stage(c, q1, new List<MessageExt> { Row(Topic, 0, MiB / 2) });
        c.DivideTopicThresholdsForTest();
        Assert.Equal(1, c.PullThresholdSizeForQueue);

        Assert.False(Hit(c, q0), "0 whole MiB of its own is inside the 1MiB share");
        Assert.False(Hit(c, q1), "the sibling's half MiB must not be added to this queue");
        Assert.Equal(0L, c.FlowControlTriggered);

        // Now q1 really is over its own share: one decision records exactly one slot.
        Stage(c, q1, new List<MessageExt> { Row(Topic, 0, 2 * MiB) });
        Assert.True(Hit(c, q1));
        Assert.Equal(1L, c.FlowControlTriggered);
        Assert.False(Hit(c, q0));
    }

    /// <summary>The byte reason is the truncated whole-MiB number with the MB suffix and no
    /// decimals: a 1.5 MiB backlog is reported as the same "size=1MB" the comparison used, so
    /// what an operator reads is what tripped. The old one-decimal format described a number
    /// the gate never computed.</summary>
    [Fact]
    public void ReasonText_ReportsTheTruncatedWholeMiB()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlFormatNet");
        var mq = QueueOf(Topic, 0);
        SizeGateOnly(c, 0);
        Stage(c, mq, new List<MessageExt> { Row(Topic, 0, (3 * MiB) / 2) });
        List<string> reasons = CaptureGateReasons(c, mq, 1);
        Assert.Single(reasons);
        Assert.Contains("size=1MB", reasons[0]);
        Assert.DoesNotContain("1.5", reasons[0]);
        Assert.DoesNotContain("MiB", reasons[0]);
        // The unit suffix sits directly on the integer: nothing culture-dependent can slip between
        // the number and its unit, and no decimals follow it.
        Assert.Contains("size=1MB, pause pull", reasons[0]);
    }

    // ---------------- the POP path's wait-ack gate ----------------

    private static bool PopHit(DefaultMQPushConsumer c, MessageQueue mq, PopProcessQueue pq)
        => c.PopFlowControlHitForTest(mq, pq);

    /// <summary>The POP gate judges one queue's own popped-but-unacked debt, strictly above the
    /// threshold: the queue exactly on the limit still pops, and a sibling is never measured against
    /// its neighbour's backlog.</summary>
    [Fact]
    public void PopGate_IsStrictlyGreaterAndReadsOnlyThisQueuesDebt()
    {
        var c = new DefaultMQPushConsumer("GID_PopFlowGateNet");
        var hot = QueueOf(Topic, 0);
        var cold = QueueOf(Topic, 1);
        c.PopThresholdForQueue = 3;
        var hotPq = new PopProcessQueue();
        var coldPq = new PopProcessQueue();

        hotPq.IncFoundMsg(3);
        Assert.False(PopHit(c, hot, hotPq), "3 awaited acks are not strictly greater than the threshold 3");
        // a round that does not trip must count nothing
        Assert.Equal(0L, c.FlowControlTriggered);
        hotPq.IncFoundMsg(1);
        Assert.True(PopHit(c, hot, hotPq), "the 4th awaited ack pauses this queue");
        Assert.Equal(1L, c.FlowControlTriggered);

        // Paired control: all 4 outstanding acks sit on ONE queue, so the empty sibling must keep
        // popping. A gate that summed the instance's debt would pause both.
        Assert.Equal(4, hotPq.WaitAckCount() + coldPq.WaitAckCount());
        Assert.False(PopHit(c, cold, coldPq), "the gate never sums its sibling queues");
        // still 1: only the hot queue tripped
        Assert.Equal(1L, c.FlowControlTriggered);

        // The shipped default is a message count, not the pull side's 1000.
        var plain = new DefaultMQPushConsumer("GID_PopFlowGateDefaultNet");
        var defaultPq = new PopProcessQueue();
        defaultPq.IncFoundMsg(plain.PopThresholdForQueue);
        Assert.False(PopHit(plain, hot, defaultPq), "exactly the default threshold still pops");
        defaultPq.IncFoundMsg(1);
        Assert.True(PopHit(plain, hot, defaultPq), "one ack past the default threshold pauses");
    }

    /// <summary>A POP trip lands on the pull path's counter, so the throttled warn cadence spans both
    /// paths. A POP gate with its own counter -- or none -- passes every pure-gate assertion above and
    /// still buries the operator in one warn per round.</summary>
    [Fact]
    public void PopHit_LandsOnThePullPathsCounter_AndJoinsItsWarnCadence()
    {
        var c = new DefaultMQPushConsumer("GID_PopFlowCounterNet");
        var mq = QueueOf(Topic, 0);
        CountGateOnly(c, 1);
        c.PopThresholdForQueue = 0;
        Stage(c, mq, Rows(Topic, 2, 100));
        var pq = new PopProcessQueue();
        pq.IncFoundMsg(1);

        Assert.True(Hit(c, mq), "2 held messages pause the pull on a gate of 1");
        Assert.Equal(1L, c.FlowControlTriggered);
        Assert.True(PopHit(c, mq, pq), "1 awaited ack pauses the pop on a threshold of 0");
        // one client-wide flow-control count spans the pull path and the POP path
        Assert.Equal(2L, c.FlowControlTriggered);

        // Round the shared counter up to 1000 on the POP path, so the next trip is the warn trip.
        for (int round = 3; round <= 1000; round++)
        {
            Assert.True(PopHit(c, mq, pq), $"pop round {round} must still pause");
        }

        Assert.Equal(1000L, c.FlowControlTriggered);
        Assert.False(DefaultMQPushConsumer.IsFlowControlWarnTrip(c.FlowControlTriggered),
            "1000 trips leave the cadence silent");
        List<string> warns = CapturePopWarns(c, mq, pq);
        Assert.Equal(1001L, c.FlowControlTriggered);
        Assert.Single(warns);
        Assert.Contains("the messages waiting to ack (1) exceeds the pop threshold 0", warns[0]);
        Assert.Contains("pause pop for 50 ms (flowControlTimes=1001)", warns[0]);
    }

    /// <summary>One POP trip, captured from the log file the gate itself writes, with the same
    /// rotation-limit care the pull-side capture takes.</summary>
    private static List<string> CapturePopWarns(DefaultMQPushConsumer c, MessageQueue mq, PopProcessQueue pq)
    {
        string dir = Path.Combine(Path.GetTempPath(),
            "rmq_dotnet_pop_fc_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        string path = Path.Combine(dir, "flow.log");
        long sizeBefore = ClientLog.LogFileMaxSize;
        int indexBefore = ClientLog.LogFileMaxIndex;
        try
        {
            ClientLog.SetLogFileLimits(256L * 1024 * 1024, 1);
            ClientLog.SetLogFile(path);
            ClientLog.SetLogLevel(LogLevel.Debug);
            Assert.True(PopHit(c, mq, pq), "this trip must pause the pop");
            ClientLog.FlushLogFile();
            string[] lines = ReadAllLinesShared(path);
            return lines.Where(l => l.Contains(" WARN  [")).ToList();
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
}
