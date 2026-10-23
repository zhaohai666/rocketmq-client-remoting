// Unit tests for the pre-pull flow-control decision -- no cluster needed.
using System.Globalization;

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

    private static MessageExt Sized(string topic, int storeSize, long queueOffset) => new()
    {
        Topic = topic,
        BrokerName = Broker,
        QueueId = 0,
        QueueOffset = queueOffset,
        StoreSize = storeSize,
    };

    private static List<MessageExt> NSized(string topic, int storeSize, int n)
        => Enumerable.Range(0, n).Select(i => Sized(topic, storeSize, i)).ToList();

    /// <summary>Turns off every gate except the message count: whichever one matches first
    /// would mask the branch under test.</summary>
    private static void GatesOffExceptCounts(DefaultMQPushConsumer c)
    {
        c.PullThresholdForQueue = int.MaxValue;
        c.PullThresholdSizeForQueue = 0;
        c.ConsumeConcurrentlyMaxSpan = 0;
        c.PullThresholdForTopic = -1;
        c.PullThresholdSizeForTopic = -1;
    }

    /// <summary>Preloads a batch of messages into some queue's buffer and registers that
    /// queue as owned (the owned-queue table is the divisor of the topic-level budgets, see
    /// TopicThresholdTests).</summary>
    private static void Stage(DefaultMQPushConsumer c, MessageQueue mq, List<MessageExt> msgs)
    {
        string key = DefaultMQPushConsumer.OffsetKeyForTest(mq);
        c.SetAssignedForTest(key, mq);
        c.SetPendingForTest(key, msgs);
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
    public void Defaults_OnlyTheCountGateIsLive()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlDefaultsNet");
        var mq = QueueOf(Topic, 0);
        // the defaults are 1000 messages / 100MiB / span 2000, and the topic level is off: a small buffer never matches
        Stage(c, mq, NSized(Topic, 100, 1));
        Assert.False(Hit(c, mq));
        Assert.Equal(0L, c.FlowControlTriggered);
        // An empty buffer (nothing pulled yet) does not match either, and the span must not come out negative
        Stage(c, mq, new List<MessageExt>());
        Assert.False(Hit(c, mq));
    }

    [Fact]
    public void CountGate_FiresAboveThreshold_AndTreatsZeroAsOne()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlCountNet");
        var mq = QueueOf(Topic, 0);
        GatesOffExceptCounts(c);
        c.PullThresholdForQueue = 3;
        Stage(c, mq, NSized(Topic, 100, 2));
        Assert.False(Hit(c, mq));
        // The gate is strictly greater than the threshold: a buffer that has reached 3 exactly is
        // still allowed to pull, so the threshold reads as "how many may be cached".
        Stage(c, mq, NSized(Topic, 100, 3));
        Assert.False(Hit(c, mq), "exactly 3 messages against a threshold of 3 must not fire (>, not >=)");
        Assert.Equal(0L, c.FlowControlTriggered);
        Stage(c, mq, NSized(Topic, 100, 4));
        Assert.True(Hit(c, mq));
        Assert.Equal(1L, c.FlowControlTriggered);
        // The max(1,n) guard makes a 0 threshold behave like 1, so 1 still fits and 2 fires.
        c.PullThresholdForQueue = 0;
        Stage(c, mq, NSized(Topic, 100, 1));
        Assert.False(Hit(c, mq), "a 0 threshold counts as 1 message, so a single message still fits");
        Stage(c, mq, NSized(Topic, 100, 2));
        Assert.True(Hit(c, mq), "PullThresholdForQueue=0 must be counted as 1 message (the max(1,n) guard)");
    }

    [Fact]
    public void SizeGate_IsInMib_AndDisabledByZero()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlSizeNet");
        var mq = QueueOf(Topic, 0);
        GatesOffExceptCounts(c);
        c.PullThresholdSizeForQueue = 1;             // 1 MiB
        Stage(c, mq, NSized(Topic, 300, 3));         // 900 B
        Assert.False(Hit(c, mq));
        // The unit is MiB rather than bytes, and the gate is strictly greater than the threshold:
        // exactly 1 MiB is still inside the budget.
        Stage(c, mq, new List<MessageExt> { Sized(Topic, MiB, 0) });
        Assert.False(Hit(c, mq), "exactly 1MiB against a 1MiB threshold must not fire (>, not >=)");
        Stage(c, mq, new List<MessageExt> { Sized(Topic, MiB + 1, 0) });
        Assert.True(Hit(c, mq), "1 byte over the 1MiB threshold must fire");
        Stage(c, mq, NSized(Topic, 2 * MiB, 4));
        Assert.True(Hit(c, mq));
        // 0 = this gate is off, no matter how large the buffer is
        c.PullThresholdSizeForQueue = 0;
        Assert.False(Hit(c, mq));
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
        GatesOffExceptCounts(c);
        c.PullThresholdForQueue = 1;
        Stage(c, mq, NSized(Topic, 100, 2));

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
            GatesOffExceptCounts(c);
            c.PullThresholdForQueue = 1;
            Stage(c, mq, NSized(Topic, 100, 2));

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

    [Fact]
    public void SpanGate_IsStrictlyGreater_AndMeasuresRealSpan()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlSpanNet");
        var mq = QueueOf(Topic, 0);
        GatesOffExceptCounts(c);
        c.ConsumeConcurrentlyMaxSpan = 10;
        Stage(c, mq, new List<MessageExt> { Sized(Topic, 1, 0), Sized(Topic, 1, 100) });
        Assert.True(Hit(c, mq));
        c.ConsumeConcurrentlyMaxSpan = 100;
        Assert.False(Hit(c, mq), "a span of 100 is not strictly greater than 100, it must not match");
        // an out-of-order buffer must still measure the real span (min/max, not first-minus-last)
        c.ConsumeConcurrentlyMaxSpan = 10;
        Stage(c, mq, new List<MessageExt>
        {
            Sized(Topic, 1, 50), Sized(Topic, 1, 5), Sized(Topic, 1, 7),
        });
        Assert.True(Hit(c, mq), "an out-of-order buffer must measure the span as max-min, not first-minus-last");
        c.ConsumeConcurrentlyMaxSpan = 0;
        Assert.False(Hit(c, mq));
    }

    /// <summary>The pull gate judges ONE queue's own buffer: a topic-level budget reaches it
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
        GatesOffExceptCounts(c);
        c.PullThresholdForTopic = 1000;
        MessageQueue[] all = { q0, q1, q2, q3 };
        foreach (MessageQueue mq in all)
        {
            Stage(c, mq, NSized(Topic, 100, 250));
        }

        // The division over the 4 owned queues is what changes, not the pull-time total.
        c.DivideTopicThresholdsForTest();
        Assert.Equal(250, c.PullThresholdForQueue);
        foreach (MessageQueue mq in all)
        {
            Assert.False(Hit(c, mq), $"{mq} sits at its own share of 250: a sibling's"
                                     + " buffer must not pause it");
        }

        Assert.Equal(0L, c.FlowControlTriggered);

        // One queue over its own share pauses, and it pauses on the COUNT reason of its own
        // buffer -- not on a topic total; its three siblings stay unpaused.
        Stage(c, q0, NSized(Topic, 100, 251));
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
        Stage(c, other, NSized(OtherTopic, 100, 250));
        Assert.False(Hit(c, other));
    }

    /// <summary>Same contract for the byte budget: the divided gate judges this queue's own
    /// bytes, so a topic-wide total over the budget is still inside the limit as long as no
    /// single queue is. With the old pull-time topic sum these two half-MiB buffers (1.2MiB
    /// across the topic against a 1MiB budget) would both pause.</summary>
    [Fact]
    public void SizeGate_JudgesOwnBytes_AndNotTheSiblingsTotal()
    {
        var c = new DefaultMQPushConsumer("GID_FlowControlNoSumSizeNet");
        MessageQueue q0 = QueueOf(Topic, 0);
        MessageQueue q1 = QueueOf(Topic, 1);
        GatesOffExceptCounts(c);
        // 2 MiB for the topic over the 2 queues this consumer owns -> 1 MiB per queue, and
        // the queue-level switch is only ever written by the division.
        c.PullThresholdSizeForTopic = 2;
        Stage(c, q0, new List<MessageExt> { Sized(Topic, MiB / 2, 0) });
        Stage(c, q1, new List<MessageExt> { Sized(Topic, MiB / 2, 0) });
        c.DivideTopicThresholdsForTest();
        Assert.Equal(1, c.PullThresholdSizeForQueue);

        Assert.False(Hit(c, q0), "0.5MiB of its own is inside the 1MiB share");
        Assert.False(Hit(c, q1), "the sibling's 0.5MiB must not be added to this queue");
        Assert.Equal(0L, c.FlowControlTriggered);

        // Now q1 really is over its own share: one decision records exactly one slot.
        Stage(c, q1, new List<MessageExt> { Sized(Topic, 2 * MiB, 0) });
        Assert.True(Hit(c, q1));
        Assert.Equal(1L, c.FlowControlTriggered);
        Assert.False(Hit(c, q0));
    }

    [Fact]
    public void ReasonText_UsesMbUnitWithOneDecimal()
    {
        // the number format in the log is locked to "%.1fMB", and the assertion parses in the same shape
        var c = new DefaultMQPushConsumer("GID_FlowControlFormatNet");
        var mq = QueueOf(Topic, 0);
        GatesOffExceptCounts(c);
        c.PullThresholdSizeForQueue = 1;
        Stage(c, mq, new List<MessageExt> { Sized(Topic, (3 * MiB) / 2, 0) });
        Assert.True(Hit(c, mq));
        // 1.5MiB must be written as "1.5MB" under InvariantCulture, with no local thousands separator or comma decimal point
        Assert.Equal("1.5", (1.5).ToString("F1", CultureInfo.InvariantCulture));
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
        GatesOffExceptCounts(c);
        c.PullThresholdForQueue = 1;
        c.PopThresholdForQueue = 0;
        Stage(c, mq, NSized(Topic, 100, 2));
        var pq = new PopProcessQueue();
        pq.IncFoundMsg(1);

        Assert.True(Hit(c, mq), "2 buffered messages pause the pull on a gate of 1");
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
