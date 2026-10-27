// Local unit tests for the POP **consumer side** (DefaultMQPushConsumer.PopMode = true),
// no cluster needed.
//
// Why a separate file: the protocol pipe passing (PopTests) does not mean the consume loop
// is right. The consumer side has its own set of easy-to-get-wrong semantics, and it gets
// them wrong **very quietly** -- for example a wrong ackIndex default makes CONSUME_SUCCESS
// ack nothing, and the broker revives and redelivers the message once invisibleTime
// expires; if the observation window is shorter than invisibleTime, a real machine still
// looks like "everything passed". So whatever can be pinned offline must be pinned first.

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class PopConsumerTests
{
    private const string Group = "GID_PopNetUnit";
    private const string Topic = "PopNetUnitTopic";
    private const string Broker = "broker-a";

    // Hand-assemble an 8-segment CK: BuildExtraInfo derives retryFlag from the topic, while here the retry segment has to be given explicitly
    private static string Ck(int queueId = 0, long offset = 0, string retry = "0",
                            long popTime = 1000, long invisible = 60000)
        => string.Format(System.Globalization.CultureInfo.InvariantCulture,
                         "0 {0} {1} 0 {2} {3} {4} {5}",
                         popTime, invisible, retry, Broker, queueId, offset);

    private static MessageExt Msg(string topic = Topic, int queueId = 0, long queueOffset = 0,
                                  string? popCk = null)
    {
        var m = new MessageExt { Topic = topic, QueueId = queueId, QueueOffset = queueOffset };
        if (popCk is not null)
        {
            m.Properties[MessageConst.PropertyPopCk] = popCk;
        }

        return m;
    }

    // ---------------------------------------------------------------- PopProcessQueue

    [Fact]
    public void ProcessQueueCountsUpAndDown()
    {
        var pq = new PopProcessQueue();
        Assert.Equal(0, pq.WaitAckCount());
        pq.IncFoundMsg(3);
        Assert.Equal(3, pq.WaitAckCount());
        pq.Ack();
        pq.Ack();
        Assert.Equal(1, pq.WaitAckCount());
        pq.DecFoundMsg(-1);
        Assert.Equal(0, pq.WaitAckCount());
    }

    [Fact]
    public void ProcessQueueDroppedFlag()
    {
        var pq = new PopProcessQueue();
        Assert.False(pq.IsDropped());
        pq.SetDropped(true);
        Assert.True(pq.IsDropped());
    }

    // ---------------------------------------------------------------- Defaults

    [Fact]
    public void PopModeOffByDefault()
    {
        // When off it must go entirely through the original pull path, behaving exactly as before the change
        Assert.False(new DefaultMQPushConsumer(Group).PopMode);
    }

    [Fact]
    public void PopDefaultsMatchLegacy()
    {
        var c = new DefaultMQPushConsumer(Group);
        Assert.Equal(60000, c.PopInvisibleTime);
        Assert.Equal(32, c.PopBatchNums);
        Assert.Equal(96, c.PopThresholdForQueue);
        // A broker-side maxMsgNums > 32 replies INVALID_PARAMETER
        Assert.True(c.PopBatchNums <= 32);
        // The long-poll suspend duration must be < the request timeout, otherwise the client times out first and every round spins on nothing
        Assert.True(c.PopPollTimeMillis < c.PopTimeoutMillis);
    }

    [Fact]
    public void DelayLevelTableIsSecondsAndAscending()
    {
        // The tier table is in seconds, the first tier is 10s (the send delay tiers start at 1s, do not mix them up)
        Assert.Equal(10, PopDelayLevelDefaults.Table[0]);
        Assert.Equal(7200, PopDelayLevelDefaults.Table[^1]);
        for (int i = 1; i < PopDelayLevelDefaults.Table.Length; i++)
        {
            Assert.True(PopDelayLevelDefaults.Table[i - 1] < PopDelayLevelDefaults.Table[i]);
        }

        Assert.Equal(5000, DefaultMQPushConsumer.MinPopInvisibleTime);
        Assert.Equal(300000, DefaultMQPushConsumer.MaxPopInvisibleTime);
    }

    // ---------------------------------------------------------------- POP_CK reconstruction

    [Fact]
    public void CkTargetNormalTopicRoundtrip()
    {
        var c = new DefaultMQPushConsumer(Group);
        MessageExt m = Msg(Topic, 3, 0, Ck(3, 7));
        PopCkTarget? t = c.PopCkTarget(m);
        Assert.NotNull(t);
        Assert.Equal(Topic, t!.Topic);
        Assert.Equal(Broker, t.BrokerName);
        Assert.Equal(3, t.QueueId);
        Assert.Equal(7, t.Offset);
    }

    [Fact]
    public void CkTargetRetryFlagOneRebuildsRetryTopic()
    {
        // retryFlag=1 => the real topic is %RETRY%<group>_<topic> (V1 uses an underscore).
        // Even when the topic on the message has already been restored by resetRetryAndNamespace,
        // as long as the CK says 1 the ack target topic must be the one with the %RETRY% prefix.
        var c = new DefaultMQPushConsumer(Group);
        MessageExt m = Msg(Topic, 1, 2, Ck(1, 2, "1"));
        PopCkTarget? t = c.PopCkTarget(m);
        Assert.NotNull(t);
        Assert.Equal("%RETRY%" + Group + "_" + Topic, t!.Topic);
        Assert.Equal(2, t.Offset);
    }

    [Fact]
    public void CkTargetRetryFlagTwoUsesPlusSeparator()
    {
        // V2 uses '+' as the separator (when enableRetryTopicV2=true)
        var c = new DefaultMQPushConsumer(Group);
        MessageExt m = Msg(Topic, 0, 0, Ck(0, 0, "2"));
        PopCkTarget? t = c.PopCkTarget(m);
        Assert.NotNull(t);
        Assert.Equal("%RETRY%" + Group + "+" + Topic, t!.Topic);
    }

    [Fact]
    public void CkTargetMissingReturnsNull()
    {
        // No CK -> give up on the ack and let the broker revive it (it must not throw)
        var c = new DefaultMQPushConsumer(Group);
        Assert.Null(c.PopCkTarget(Msg()));
    }

    [Fact]
    public void CkTargetShortSegmentsReturnsNull()
    {
        // Not enough segments (7) -> give up on the ack, it must not throw
        var c = new DefaultMQPushConsumer(Group);
        string seven = ExtraInfoUtil.BuildExtraInfo(0, 1, 2, 3, Topic, Broker, 4);
        Assert.Null(c.PopCkTarget(Msg(Topic, 4, 0, seven)));
    }

    [Fact]
    public void CkTargetGarbageReturnsNull()
    {
        var c = new DefaultMQPushConsumer(Group);
        Assert.Null(c.PopCkTarget(Msg(Topic, 0, 0, "only two")));
    }

    // ---------------------------------------------------------------- Timeout decision

    [Fact]
    public void UnparsableCkCountsAsTimeout()
    {
        Assert.True(DefaultMQPushConsumer.IsPopTimeout(0, 0));
        Assert.True(DefaultMQPushConsumer.IsPopTimeout(0, 60000));
        Assert.True(DefaultMQPushConsumer.IsPopTimeout(1000, 0));
    }

    [Fact]
    public void WithinWindowIsNotTimeout()
    {
        long now = UtilAll.CurrentTimeMillis();
        Assert.False(DefaultMQPushConsumer.IsPopTimeout(now, 60000));
        Assert.True(DefaultMQPushConsumer.IsPopTimeout(now - 60001, 60000));
    }

    // ------------------------------------------------- pullRT/pullTPS of the POP loop

    private static PopResult Popped(int n)
    {
        var r = new PopResult { Status = PopStatus.Found };
        for (int i = 0; i < n; i++)
        {
            r.MsgFoundList.Add(Msg(Topic, i, i));
        }

        return r;
    }

    [Fact]
    public void RecordPopPullStats_FoundRecordsRtThenTps()
    {
        var stats = new ConsumerStatsManager();
        string key = Topic + "@" + Group;
        // rewound 30ms: RT = now - began, so the positive assertion of >0 does not depend on scheduling precision
        long began = UtilAll.CurrentTimeMillis() - 30;

        DefaultMQPushConsumer.RecordPopPullStats(stats, Group, Topic, Popped(2), began);

        (long rtValue, long rtTimes) = stats.TopicAndGroupPullRT.Find(key)!.Snapshot();
        (long tpsValue, long tpsTimes) = stats.TopicAndGroupPullTPS.Find(key)!.Snapshot();
        Assert.Equal(1, rtTimes);
        Assert.True(rtValue >= 30, $"RT should be the elapsed time of this round's pop, actually got {rtValue}");
        Assert.Equal(1, tpsTimes);
        Assert.Equal(2, tpsValue);
    }

    [Fact]
    public void RecordPopPullStats_FoundWithEmptyListRecordsRtOnly()
    {
        var stats = new ConsumerStatsManager();
        string key = Topic + "@" + Group;
        long began = UtilAll.CurrentTimeMillis() - 30;

        DefaultMQPushConsumer.RecordPopPullStats(stats, Group, Topic, Popped(0), began);

        Assert.Equal(1, stats.TopicAndGroupPullRT.Find(key)!.Snapshot().times);
        // TPS must not move at all: counting 0 messages in the numerator only drags the average down
        Assert.Null(stats.TopicAndGroupPullTPS.Find(key));
    }

    [Fact]
    public void RecordPopPullStats_PollingNotFoundRecordsNothing()
    {
        // PULL_RT is one round's own client-side trip time, so it is only meaningful for a round
        // that came back FOUND. A POLLING_NOT_FOUND answer -- even one carrying rows -- books
        // neither RT nor TPS.
        var stats = new ConsumerStatsManager();
        string key = Topic + "@" + Group;

        DefaultMQPushConsumer.RecordPopPullStats(
            stats, Group, Topic,
            new PopResult { Status = PopStatus.PollingNotFound, MsgFoundList = { Msg(Topic, 0, 0) } },
            UtilAll.CurrentTimeMillis() - 30);

        Assert.Null(stats.TopicAndGroupPullRT.Find(key));
        Assert.Null(stats.TopicAndGroupPullTPS.Find(key));
    }

    /// <summary>The loop runs the whole pre-delivery pipeline FIRST and replaces
    /// the answer's list with the kept rows before stats are booked, so TPS can
    /// never see the rows that were dropped. Control: an implementation that
    /// books stats on the raw popped list reads TPS 3 here and goes red.</summary>
    [Fact]
    public void RecordPopPullStats_TpsCountsOnlyTheKeptRows()
    {
        var stats = new ConsumerStatsManager();
        string key = Topic + "@" + Group;
        PopResult popped = Popped(3);
        // Pipeline verdict: of the 3 popped rows only 1 survives.
        popped.MsgFoundList = popped.MsgFoundList.GetRange(0, 1);

        DefaultMQPushConsumer.RecordPopPullStats(stats, Group, Topic, popped,
            UtilAll.CurrentTimeMillis() - 30);

        (long tpsValue, long tpsTimes) = stats.TopicAndGroupPullTPS.Find(key)!.Snapshot();
        Assert.Equal(1, tpsTimes);
        Assert.Equal(1, tpsValue);
        Assert.Equal(1, stats.TopicAndGroupPullRT.Find(key)!.Snapshot().times);
    }

    /// <summary>A Found answer whose rows were ALL filtered out still books its
    /// round-trip time, books no TPS, and (kept count 0) submits no work.</summary>
    [Fact]
    public void RecordPopPullStats_FoundWithNothingKept_BooksRtButNoTps()
    {
        var stats = new ConsumerStatsManager();
        string key = Topic + "@" + Group;
        PopResult popped = Popped(3);
        popped.MsgFoundList = new List<MessageExt>();

        DefaultMQPushConsumer.RecordPopPullStats(stats, Group, Topic, popped,
            UtilAll.CurrentTimeMillis() - 30);

        Assert.Equal(1, stats.TopicAndGroupPullRT.Find(key)!.Snapshot().times);
        Assert.Null(stats.TopicAndGroupPullTPS.Find(key));
    }

    // ------------------------ POP re-schedule decision (pure) ------------------------

    [Fact]
    public void PopNextRound_FoundWithKeptRows_PacesByPullIntervalOrGoesStraightOn()
    {
        // Interval 0: no pace to wait, next round now.
        Assert.Equal(0, DefaultMQPushConsumer.PopNextRoundDelayMillis(PopStatus.Found, 2, 0));
        // Interval > 0: the configured pace applies to a real delivery.
        Assert.Equal(50, DefaultMQPushConsumer.PopNextRoundDelayMillis(PopStatus.Found, 2, 50));
    }

    [Fact]
    public void PopNextRound_FoundWithZeroKept_GoesStraightOn_EvenWhenIntervalIsSet()
    {
        // A batch fully filtered out is an empty answer: the interval only paces
        // real deliveries.
        Assert.Equal(0, DefaultMQPushConsumer.PopNextRoundDelayMillis(PopStatus.Found, 0, 50));
    }

    [Fact]
    public void PopNextRound_EmptyAnswers_GoStraightOn()
    {
        Assert.Equal(0, DefaultMQPushConsumer.PopNextRoundDelayMillis(PopStatus.NoNewMsg, 0, 50));
        Assert.Equal(0,
            DefaultMQPushConsumer.PopNextRoundDelayMillis(PopStatus.PollingNotFound, 0, 50));
    }

    [Fact]
    public void PopNextRound_PollingFullAndUnknown_BackOffThreeSeconds()
    {
        Assert.Equal(3000, DefaultMQPushConsumer.PopExceptionBackoffMillis);
        // Strictly above the 200 ms hot-round nap: the loop must never let that
        // guard replace this back-off.
        Assert.True(DefaultMQPushConsumer.PopNextRoundDelayMillis(
            PopStatus.PollingFull, 0, 50) > 200);
        Assert.Equal(DefaultMQPushConsumer.PopExceptionBackoffMillis,
            DefaultMQPushConsumer.PopNextRoundDelayMillis(PopStatus.PollingFull, 0, 50));
        // Any status the decision does not know falls into the same back-off.
        Assert.Equal(DefaultMQPushConsumer.PopExceptionBackoffMillis,
            DefaultMQPushConsumer.PopNextRoundDelayMillis((PopStatus)42, 0, 0));
    }

    // ------------------------ POP-only reconsume budget drop ------------------------

    private static MessageExt Budgeted(int reconsumeTimes, long queueOffset)
    {
        var m = Msg(Topic, 0, queueOffset);
        m.ReconsumeTimes = reconsumeTimes;
        return m;
    }

    /// <summary>Budget 2 with rows at reconsume counts 1, 2 and 3 must drop ONLY
    /// the 3: the test is strict "greater than", a "greater-or-equal" reading
    /// would also drop the row AT the budget.</summary>
    [Fact]
    public void BudgetDrop_RemovesOnlyRowsStrictlyOverTheBudget()
    {
        var c = new DefaultMQPushConsumer(Group) { MaxReconsumeTimes = 2 };
        var rows = new List<MessageExt> { Budgeted(1, 0), Budgeted(2, 1), Budgeted(3, 2) };

        List<MessageExt> kept = c.DropOverReconsumeBudget(rows);

        Assert.Equal(new[] { 1, 2 }, kept.Select(m => m.ReconsumeTimes).ToArray());
        Assert.Equal(new long[] { 0, 1 }, kept.Select(m => m.QueueOffset).ToArray());
    }

    /// <summary>The budget knob is the CONCURRENT-side reading: -1 means the
    /// broker default 16, not unlimited. Control: an implementation that reuses
    /// the orderly-side helper (-1 -> int.MaxValue) keeps the row at 17 and goes
    /// red.</summary>
    [Fact]
    public void BudgetDrop_MinusOneMeansSixteen_NotUnlimited()
    {
        var c = new DefaultMQPushConsumer(Group);
        Assert.Equal(-1, c.MaxReconsumeTimes);
        Assert.Equal(16, c.MaxReconsumeTimesOrDefault());

        List<MessageExt> kept = c.DropOverReconsumeBudget(
            new List<MessageExt> { Budgeted(16, 0), Budgeted(17, 1) });

        Assert.Single(kept);
        Assert.Equal(16, kept[0].ReconsumeTimes);
    }

    [Fact]
    public void BudgetDrop_EmptyInputStaysEmpty()
    {
        var c = new DefaultMQPushConsumer(Group) { MaxReconsumeTimes = 2 };
        Assert.Empty(c.DropOverReconsumeBudget(new List<MessageExt>()));
    }

    /// <summary>The budget step is POP-only. The shared pre-delivery filter must
    /// leave an over-budget row untouched -- that helper is what the pull path
    /// runs, and the pull path still delivers the row. Control: hoisting the
    /// budget drop into the shared helper removes the row here and goes red.</summary>
    [Fact]
    public void SharedFilter_DoesNotApplyTheReconsumeBudget()
    {
        var c = new DefaultMQPushConsumer(Group) { MaxReconsumeTimes = 2 };
        SubscriptionData subAll = FilterAPI.BuildSubscriptionData(Topic, "*");

        List<MessageExt> kept = c.FilterMessagesForDelivery(new MessageQueue(Topic, Broker, 0),
            subAll, new List<MessageExt> { Budgeted(3, 0) });

        Assert.Single(kept);
        Assert.Equal(3, kept[0].ReconsumeTimes);
    }

    /// <summary>Every budget drop books one INFO line naming the message's
    /// reconsume count and saying the message is being acked.</summary>
    [Fact]
    public void BudgetDrop_LogsOneInfoLinePerDrop()
    {
        var c = new DefaultMQPushConsumer(Group) { MaxReconsumeTimes = 2 };
        var rows = new List<MessageExt> { Budgeted(1, 0), Budgeted(5, 1) };

        List<string> info = CaptureInfo(() =>
        {
            List<MessageExt> kept = c.DropOverReconsumeBudget(rows);
            Assert.Single(kept);
        });

        Assert.Single(info);
        Assert.Contains("reconsumeTimes=5", info[0]);
        Assert.Contains("acked", info[0]);
    }

    // ------------------------------------------------ INFO line capture

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

    /// <summary>Run <paramref name="work" /> with the log pointed at a private
    /// file and return every INFO line it wrote; the sentinel proves the capture
    /// works, so an empty result really means "nothing was logged".</summary>
    private static List<string> CaptureInfo(Action work)
    {
        string dir = Path.Combine(Path.GetTempPath(),
            "rmq_dotnet_popbudget_" + Guid.NewGuid().ToString("N")[..8]);
        Directory.CreateDirectory(dir);
        string path = Path.Combine(dir, "popbudget.log");
        long sizeBefore = ClientLog.LogFileMaxSize;
        int indexBefore = ClientLog.LogFileMaxIndex;
        try
        {
            ClientLog.SetLogFileLimits(256L * 1024 * 1024, 1);
            ClientLog.SetLogFile(path);
            ClientLog.SetLogLevel(LogLevel.Info);
            work();
            const string Sentinel = "popbudget-capture-sentinel";
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

    // ------------------------ POP retry-topic prefix strip at delivery ------------------------

    /// <summary>A row as it arrives at the concurrent POP dispatch: fresh CK so
    /// the batch is not abandoned as timed out.</summary>
    private static MessageExt RetryRow(string topic, long queueOffset)
    {
        var m = new MessageExt
        {
            Topic = topic,
            QueueId = 0,
            QueueOffset = queueOffset,
            Body = new byte[] { 1 },
        };
        m.Properties[MessageConst.PropertyPopCk] = ExtraInfoUtil.BuildExtraInfo(
            queueOffset, UtilAll.CurrentTimeMillis(), 60000, 0, topic, Broker, 0, queueOffset);
        return m;
    }

    private sealed class TopicRecordingListener : IMessageListenerConcurrently
    {
        public readonly List<string> SeenTopics = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
        {
            foreach (MessageExt m in msgs)
            {
                SeenTopics.Add(m.Topic);
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    /// <summary>Both POP wire spellings of THIS group are rewritten to the
    /// business topic, and nothing else is touched: the classic
    /// <c>%RETRY%&lt;group&gt;</c>, another group's underscore form and a wrapper
    /// with nothing after the separator all reach the listener as they
    /// arrived.</summary>
    [Fact]
    public void PopDispatch_StripsBothWireSpellingsOfThisGroup()
    {
        var listener = new TopicRecordingListener();
        var c = new DefaultMQPushConsumer(Group) { PopMode = true };
        c.SetMessageListener(listener);
        var pq = new PopProcessQueue();
        pq.IncFoundMsg(6);

        c.SubmitPopConsumeRequest(new List<MessageExt>
        {
            RetryRow("%RETRY%" + Group + "_BizTopic", 0),
            RetryRow("%RETRY%" + Group + "+BizTopic", 1),
            RetryRow("%RETRY%" + Group + "+Biz_Topic", 2),
            RetryRow(MixAll.GetRetryTopic(Group), 3),
            RetryRow("%RETRY%GID_SomeOtherUnit_BizTopic", 4),
            RetryRow("%RETRY%" + Group + "+", 5),
        }, pq, new MessageQueue(Topic, Broker, 0));

        Assert.Equal(new[]
        {
            "BizTopic",
            "BizTopic",
            "Biz_Topic",
            "%RETRY%" + Group,
            "%RETRY%GID_SomeOtherUnit_BizTopic",
            "%RETRY%" + Group + "+",
        }, listener.SeenTopics.ToArray());
    }

    /// <summary>The pull-mode concurrent entry strips too, ahead of the
    /// property-based restore: a wrapper naming this group becomes the business
    /// topic, and a bare <c>%RETRY%&lt;group&gt;</c> row still restores from its
    /// RETRY_TOPIC property.</summary>
    [Fact]
    public void PullDispatch_StripsThePopRetryPrefixBeforeEnqueue()
    {
        var c = new DefaultMQPushConsumer(Group);

        var wrapped = RetryRow("%RETRY%" + Group + "+BizTopic", 0);
        var classic = RetryRow(MixAll.GetRetryTopic(Group), 1);
        classic.Properties[MessageConst.PropertyRetryTopic] = Topic;
        var foreign = RetryRow("%RETRY%GID_SomeOtherUnit_BizTopic", 2);

        List<MessageExt> rows = new List<MessageExt> { wrapped, classic, foreign };
        c.PreparePullRowsForDelivery(rows, false);

        Assert.Equal(new[] { "BizTopic", Topic, "%RETRY%GID_SomeOtherUnit_BizTopic" },
            rows.Select(m => m.Topic).ToArray());
    }

    /// <summary>Control for the entry above: an orderly consumer keeps the
    /// wrapper name, because it consumes the queue it locked under that name and
    /// the offset bookkeeping of a retry queue is keyed on it.</summary>
    [Fact]
    public void PullDispatch_OrderlyKeepsThePopRetryPrefix()
    {
        var c = new DefaultMQPushConsumer(Group);
        List<MessageExt> rows = new List<MessageExt>
        {
            RetryRow("%RETRY%" + Group + "_BizTopic", 0),
        };

        c.PreparePullRowsForDelivery(rows, true);

        Assert.Equal("%RETRY%" + Group + "_BizTopic", Assert.Single(rows).Topic);
    }

    /// <summary>Once the group is registered with the namespace folded in, both
    /// spellings of this consumer's name are recognised: the wrapper naming the
    /// namespaced group and the one naming the plain group. Either way the
    /// listener ends up with the plain business topic -- the wrapper is cut here
    /// and the topic's own namespace prefix comes off one step later, with the
    /// same step that every other delivered row is unwrapped by.</summary>
    [Fact]
    public void PopDispatch_StripsEitherSpellingOfThisGroupName()
    {
        var listener = new TopicRecordingListener();
        var c = new DefaultMQPushConsumer(NamespaceUtil.WrapNamespace("NS", Group))
        {
            PopMode = true,
            Namespace = "NS",
        };
        c.SetMessageListener(listener);
        var pq = new PopProcessQueue();
        pq.IncFoundMsg(2);

        c.SubmitPopConsumeRequest(new List<MessageExt>
        {
            RetryRow("%RETRY%NS%" + Group + "_BizTopic", 0),
            RetryRow("%RETRY%" + Group + "_NS%BizTopic", 1),
        }, pq, new MessageQueue(Topic, Broker, 0));

        Assert.Equal(new[] { "BizTopic", "BizTopic" }, listener.SeenTopics.ToArray());
    }

    /// <summary>Under a namespace, every delivered row is handed over unwrapped,
    /// not only a row restored from the RETRY_TOPIC property: a plain
    /// <c>ns%Topic</c> row would otherwise make the listener branch on a name it
    /// never subscribed to.</summary>
    [Fact]
    public void PopDispatch_UnwrapsTheNamespaceOfEveryRow()
    {
        var listener = new TopicRecordingListener();
        var c = new DefaultMQPushConsumer(NamespaceUtil.WrapNamespace("NS", Group))
        {
            PopMode = true,
            Namespace = "NS",
        };
        c.SetMessageListener(listener);
        var pq = new PopProcessQueue();
        pq.IncFoundMsg(1);

        c.SubmitPopConsumeRequest(new List<MessageExt>
        {
            RetryRow("NS%" + Topic, 0),
        }, pq, new MessageQueue(Topic, Broker, 0));

        Assert.Equal(Topic, Assert.Single(listener.SeenTopics));
    }

    /// <summary>The classic retry row keeps its original restore mechanism
    /// (property-based, topic-equality-gated): the new strip must not break it
    /// and the restore must not start firing on rows whose topic it no longer
    /// matches after stripping.</summary>
    [Fact]
    public void PopDispatch_ClassicRetryRow_StillRestoresFromTheProperty()
    {
        var listener = new TopicRecordingListener();
        var c = new DefaultMQPushConsumer(Group) { PopMode = true };
        c.SetMessageListener(listener);
        var pq = new PopProcessQueue();
        pq.IncFoundMsg(1);

        MessageExt row = RetryRow(MixAll.GetRetryTopic(Group), 0);
        row.Properties[MessageConst.PropertyRetryTopic] = Topic;
        c.SubmitPopConsumeRequest(new List<MessageExt> { row }, pq,
            new MessageQueue(Topic, Broker, 0));

        Assert.Equal(Topic, Assert.Single(listener.SeenTopics));
    }

    /// <summary>The consume-directly path must NOT strip: a
    /// <c>%RETRY%&lt;group&gt;_&lt;topic&gt;</c> row reaches the listener with
    /// its topic intact. Control: adding the strip to this entry rewrites it to
    /// the business topic and goes red.</summary>
    [Fact]
    public void ConsumeDirectly_DoesNotStripThePopRetryPrefix()
    {
        var listener = new TopicRecordingListener();
        var c = new DefaultMQPushConsumer(Group) { PopMode = true };
        c.SetMessageListener(listener);

        ConsumeMessageDirectlyResult result = c.ConsumeMessageDirectly(
            Msg("%RETRY%" + Group + "_BizTopic", 0, 0), Broker);

        Assert.Equal("CR_SUCCESS", result.ConsumeResult);
        Assert.Equal("%RETRY%" + Group + "_BizTopic", Assert.Single(listener.SeenTopics));
    }
}
