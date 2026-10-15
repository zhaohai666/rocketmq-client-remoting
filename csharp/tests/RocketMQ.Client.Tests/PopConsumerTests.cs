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
        // An empty POP poll is normal (the long poll returns after being suspended for pollTime), and counting it in RT means diluting
        // the average pull time with the suspend duration -- pullRT on the dashboard becomes completely meaningless.
        var stats = new ConsumerStatsManager();
        string key = Topic + "@" + Group;

        DefaultMQPushConsumer.RecordPopPullStats(
            stats, Group, Topic,
            new PopResult { Status = PopStatus.PollingNotFound, MsgFoundList = { Msg(Topic, 0, 0) } },
            UtilAll.CurrentTimeMillis() - 30);

        Assert.Null(stats.TopicAndGroupPullRT.Find(key));
        Assert.Null(stats.TopicAndGroupPullTPS.Find(key));
    }
}
