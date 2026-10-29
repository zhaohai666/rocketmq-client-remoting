// Unit tests for the lightweight pull consumer (DefaultLitePullConsumer) and the
// 220/221/309 bodies (covering the main pull flow and the core offset table scenarios).
using System.Reflection;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class LitePullTests
{
    /// <summary>This version of xunit's Assert.Equal takes no because argument, so numeric
    /// assertions all go through this wrapper.</summary>
    private static void Eq(long expected, long actual, string because) =>
        Assert.True(actual == expected, $"expected {expected}, actual {actual}, because {because}");

    private static bool WaitFor(Func<bool> cond, int timeoutMs = 15000)
    {
        long deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (cond()) return true;
            Thread.Sleep(20);
        }

        return cond();
    }

    private static MessageExt Buffered(MessageQueue mq, long queueOffset) => new()
    {
        Topic = mq.Topic,
        BrokerName = mq.BrokerName,
        QueueId = mq.QueueId,
        QueueOffset = queueOffset,
        Body = new byte[8],
    };

    /// <summary>The last decoded heartbeat that mentions this consumer group, or null
    /// when none has arrived yet.</summary>
    private static HeartbeatData? LastHeartbeatFor(MockCluster cluster, string group)
    {
        foreach (WireRecord rec in cluster.Records().AsEnumerable().Reverse())
        {
            if (rec.Code != RequestCode.HeartBeat) continue;
            if (!HeartbeatData.Decode(rec.Body, out HeartbeatData hb)) continue;
            if (hb.ConsumerDataSet.Any(cd => cd.GroupName == group)) return hb;
        }

        return null;
    }

    /// <summary>Pushes one heartbeat now. The background loop's 5s period would otherwise
    /// turn "what does the rebuilt set announce" into a wait on the clock.</summary>
    private static void SendHeartbeatNow(DefaultLitePullConsumer c) =>
        typeof(DefaultLitePullConsumer)
            .GetMethod("SendHeartbeatToAllBroker", BindingFlags.NonPublic | BindingFlags.Instance)!
            .Invoke(c, null);

    // ---------------------------------------------- Pure state behaviour before start

    [Fact]
    public void Constructor_DefaultGroup()
    {
        var c = new DefaultLitePullConsumer();
        Assert.Equal(MixAll.DefaultConsumerGroup, c.ConsumerGroup);
        Assert.False(c.IsStarted);
    }

    [Fact]
    public void Start_WithoutSubscriptionOrNamesrv_Throws()
    {
        var c = new DefaultLitePullConsumer();
        // No namesrv configured before start: it must report an error explicitly (it must not silently spin up an idle thread)
        Assert.ThrowsAny<MQClientException>(c.Start);
    }

    [Fact]
    public void ConsumeTimestamp_DefaultsToLegacyWallClockFormat()
    {
        var c = new DefaultLitePullConsumer();
        Assert.Equal(14, c.ConsumeTimestamp.Length);
        Assert.All(c.ConsumeTimestamp, ch => Assert.True(char.IsDigit(ch)));
    }

    [Fact]
    public void Start_RejectsEpochLookingConsumeTimestamp()
    {
        // The group name must be legal and must not be the reserved DEFAULT_CONSUMER: checkConfig comes before the starting-point check, so using the default group would only exercise the group-name error.
        var c = new DefaultLitePullConsumer("LitePullTsGroup");
        c.SetNamesrvAddr("127.0.0.1:1");
        c.Subscribe("MyTopic", "*");
        // A purely numeric epoch millisecond value must be rejected (the old implementation interpreted it as an epoch and silently computed a wrong starting point)
        c.SetConsumeTimestamp("1700000000000");
        var ex = Assert.Throws<MQClientException>(c.Start);
        Assert.Contains("consumeTimestamp is invalid", ex.Message);
    }

    [Fact]
    public void Start_KeepsValidConsumeTimestamp()
    {
        var c = new DefaultLitePullConsumer("LitePullTsGroup");
        c.SetNamesrvAddr("127.0.0.1:1");
        c.Subscribe("MyTopic", "*");
        c.SetConsumeTimestamp("20230101000000");
        // A legal value must not be killed by this startup guard (an unreachable namesrv is a different matter)
        var ex = Record.Exception(c.Start);
        Assert.DoesNotContain("consumeTimestamp is invalid", ex?.Message ?? string.Empty);
        c.Shutdown();
    }

    [Fact]
    public void LiteSurface_RefusesMisuse()
    {
        var c = new DefaultLitePullConsumer();
        var mq = new MessageQueue("MyTopic", "broker-a", 3);

        // An empty assign would revoke every queue held so far, which is what an empty
        // route lookup does by accident: it is refused and leaves the state alone.
        Assert.Throws<ArgumentException>(() => c.Assign(Array.Empty<MessageQueue>()));
        Assert.Throws<ArgumentException>(() => c.Assign(null!));
        Assert.Empty(c.Assignment());

        // A topic named "" would be registered and looked up on the wire as-is.
        Assert.Throws<ArgumentException>(() => c.Subscribe("", "TagA"));
        Assert.Throws<ArgumentException>(() => c.Subscribe("   ", "TagA"));

        // A blank assign expression would quietly mean "everything": the caller would
        // never learn its filter did not apply.
        Assert.Throws<ArgumentException>(() => c.SetSubExpressionForAssign("MyTopic", "  "));
        // A topic named "" would key that expression on "" instead.
        Assert.Throws<ArgumentException>(() => c.SetSubExpressionForAssign("   ", "TagA"));

        // An empty poll and an unknown committed offset look exactly like a queue at its
        // tail, so a consumer that never started refuses instead of answering.
        Assert.Throws<MQClientException>(() => c.Committed(mq));
        Assert.Throws<MQClientException>(() => c.Poll(20));
        // The state check comes first: an unstarted consumer is reported as that, not as
        // a timeout problem.
        Assert.Throws<MQClientException>(() => c.Poll(-1));

        // Assigning for real and then starting is still the supported path; pause and
        // resume move local state only and stay allowed before start. Seek needs a running
        // consumer, so before start it reports that rather than pinning a cursor.
        var assigned = new DefaultLitePullConsumer();
        assigned.Assign(new[] { mq });
        Assert.Single(assigned.Assignment());
        Assert.Equal(3, assigned.Assignment()[0].QueueId);
        assigned.Pause(new[] { mq });
        Assert.True(assigned.IsPausedForTest(mq));
        assigned.Resume(new[] { mq });
        Assert.False(assigned.IsPausedForTest(mq));
        var notRunning = Assert.Throws<MQClientException>(() => assigned.Seek(mq, 42));
        Assert.Equal("The consumer not running, please start it first.", notRunning.Message);

        // A subscribe consumer is a separate shape and can resubscribe its own topic.
        var subscribed = new DefaultLitePullConsumer();
        subscribed.Subscribe("MyTopic", "TagA || TagB");
        subscribed.Subscribe("MyTopic", "TagC");
        Assert.Throws<MQClientException>(() => subscribed.Assign(new[] { mq }));

        // Running: the timeout is the caller's to get right, and the assign expression
        // is a pre-start setting -- the broker already knows the old one.
        assigned.SetAutoCommit(false);
        assigned.MarkRunningForTest();
        Assert.Throws<ArgumentException>(() => assigned.Poll(-1));
        Assert.Throws<MQClientException>(() => assigned.SetSubExpressionForAssign("MyTopic", "TagA"));
        Assert.Throws<MQClientException>(() => assigned.SetSubExpressionForAssign("MyTopic", "*"));
        Assert.Empty(assigned.Poll(20));
        Eq(-1L, assigned.Committed(mq), "a running consumer with no client answers -1, not an error");
    }

    // ---------------------------------------------- The three offset tables (read/write, overwrite and guards)

    /// <summary>
    /// The core of #68: the pull cursor / the consumed cursor / the in-memory offset table
    /// are three different things and a commit may only go through the third, while the
    /// data source of commitAll is the **consumed** cursor. No network is involved here:
    /// seek and commit(map) fill each of the three tables, then the guards (-1, not held
    /// by this instance), empty arguments and the "remove unused mq" cleanup of persistAll
    /// are verified.
    /// </summary>
    [Fact]
    public void LitePullOffsetTable_ThreeTablesGuardsWithoutNetwork()
    {
        var c = new DefaultLitePullConsumer("LitePullOffsetTableGroup");
        var q0 = new MessageQueue("MyTopic", "broker-a", 0);
        var q1 = new MessageQueue("MyTopic", "broker-a", 1);
        var stranger = new MessageQueue("MyTopic", "broker-a", 7);
        c.Assign(new[] { q0, q1 });
        // The offset surface only answers for a running consumer; nothing below reaches
        // the wire, the tables are all local.
        c.SetAutoCommit(false);
        c.MarkRunningForTest();

        Assert.Equal(-1L, c.PullCursorOf(q0));
        Assert.Equal(-1L, c.ConsumeCursorOf(q0));

        c.Commit();
        Assert.Equal(-1L, c.Committed(q0));

        // Commit(map) only writes the commit landing point, neither cursor moves
        c.Commit(new Dictionary<MessageQueue, long> { [q0] = 5, [q1] = 8 }, persist: false);
        Assert.Equal(5L, c.Committed(q0));
        Assert.Equal(8L, c.Committed(q1));
        Assert.Equal(-1L, c.PullCursorOf(q0));
        Assert.Equal(-1L, c.ConsumeCursorOf(q0));

        c.Commit(new Dictionary<MessageQueue, long> { [q0] = -1, [stranger] = 3 }, persist: false);
        Eq(5L, c.Committed(q0), "an offset of -1 is only logged, it does not overwrite an existing offset");
        Eq(-1L, c.Committed(stranger), "a queue that was not assigned is not committed on its behalf");

        c.Commit(new Dictionary<MessageQueue, long>(), persist: false);
        Eq(5L, c.Committed(q0), "an empty map ignores this commit");
        c.Commit(Array.Empty<MessageQueue>(), persist: false);
        Eq(5L, c.Committed(q0), "an empty set ignores this commit");

        // Commit(Set) takes the consumed cursor, not any given value: nothing was delivered => the guard stops it
        c.Commit(new[] { q0 }, persist: false);
        Eq(5L, c.Committed(q0), "Commit(Set) does not write -1 when there is no delivery record");

        c.SetSeekBoundsForTest(0, 100);
        c.Seek(q0, 2);
        Assert.Equal(2L, c.PullCursorOf(q0));
        Eq(2L, c.ConsumeCursorOf(q0), "seek must also move the consumed cursor, otherwise a replayed range would be skipped by the old offset");

        c.Assign(new[] { q0 });
        Assert.Equal(-1L, c.PullCursorOf(q1));
        Assert.Equal(-1L, c.ConsumeCursorOf(q1));
        Eq(8L, c.Committed(q1), "assign mode does not touch offsetStore");
        c.Commit(new Dictionary<MessageQueue, long> { [q1] = 99 }, persist: false);
        Eq(8L, c.Committed(q1), "a withdrawn queue does not get its offset changed on its behalf");

        c.Commit(new[] { q0 }, persist: true);
        Eq(2L, c.Committed(q0), "Commit(Set) commits the consumed cursor");
        Eq(-1L, c.Committed(q1), "persistAll drops an unnamed queue from the in-memory table");
        Eq(2L, c.PendingCommitOf(q0), "a named queue stays in the table");
    }

    // ---------------------------------------------- 221 GetConsumerStatusBody

    [Fact]
    public void GetConsumerStatusBody_WireShape()
    {
        var b = new GetConsumerStatusBody();
        b.MessageQueueTable[new MessageQueue("MyTopic", "broker-a", 3)] = 42;
        b.MessageQueueTable[new MessageQueue("MyTopic", "broker-a", 1)] = 7;

        string json = System.Text.Encoding.UTF8.GetString(b.Encode());
        // The key is fastjson2-style inline MessageQueue JSON (alphabetical: brokerName, queueId, topic), serialised as an **escaped string key** (the same encoding as the already machine-verified 307 MqTable)
        Assert.Contains(
            "\"{\\\"brokerName\\\":\\\"broker-a\\\",\\\"queueId\\\":3,\\\"topic\\\":\\\"MyTopic\\\"}\":42",
            json);
        Assert.Contains("\"consumerTable\":{}", json);
        Assert.Contains("\"messageQueueTable\":", json);
    }

    // ---------------------------------------------- 309 ConsumeMessageDirectlyResult

    [Fact]
    public void ConsumeMessageDirectlyResult_DefaultWireShape()
    {
        var def = new ConsumeMessageDirectlyResult();
        Assert.False(def.Order);
        Assert.True(def.AutoCommit);

        string json = System.Text.Encoding.UTF8.GetString(def.Encode());
        Assert.Contains("\"order\":false", json);
        Assert.Contains("\"autoCommit\":true", json);
        Assert.Contains("\"consumeResult\":null", json);
        Assert.Contains("\"remark\":null", json);
        Assert.Contains("\"spentTimeMills\":0", json);
    }

    [Fact]
    public void ConsumeMessageDirectlyResult_RoundTrip()
    {
        var ok = new ConsumeMessageDirectlyResult { ConsumeResult = "CR_SUCCESS", SpentTimeMills = 12 };
        Assert.Contains("\"consumeResult\":\"CR_SUCCESS\"",
            System.Text.Encoding.UTF8.GetString(ok.Encode()));

        var bad = new ConsumeMessageDirectlyResult
        {
            Order = true,
            ConsumeResult = "CR_THROW_EXCEPTION",
            Remark = "std::exception: boom",
            SpentTimeMills = 33,
        };
        Assert.True(ConsumeMessageDirectlyResult.Decode(bad.Encode(), out ConsumeMessageDirectlyResult back));
        Assert.True(back.Order);
        Assert.True(back.AutoCommit);   // the round trip keeps the default true
        Assert.Equal("CR_THROW_EXCEPTION", back.ConsumeResult);
        Assert.Equal("std::exception: boom", back.Remark);
        Assert.Equal(33, back.SpentTimeMills);
    }

    // The auto-commit interval has a floor, and a floor here is an acceptance rule rather than a
    // clamp: a shorter setting is ignored, so the interval already running keeps running.
    [Fact]
    public void AutoCommitInterval_FloorIsAnAcceptanceRuleNotAClamp()
    {
        var c = new DefaultLitePullConsumer();
        Eq(5000, c.AutoCommitIntervalMillis, "the interval starts at its default");

        foreach (var rejected in new[] { 999, 0, -1 })
        {
            c.SetAutoCommitIntervalMillis(rejected);
            Eq(5000, c.AutoCommitIntervalMillis, $"a {rejected} ms setting is ignored, not clamped to the floor");
        }

        c.SetAutoCommitIntervalMillis(DefaultLitePullConsumer.MinAutoCommitIntervalMillis);
        Eq(1000, c.AutoCommitIntervalMillis, "the floor itself is accepted");

        c.SetAutoCommitIntervalMillis(2500);
        c.SetAutoCommitIntervalMillis(500);
        Eq(2500, c.AutoCommitIntervalMillis, "a rejected setting leaves the accepted interval running");
    }

    // ---------------------------------------------------------------- delivery-shape contract

    [Fact]
    public void SubscribeAndAssign_FirstShapeWins_AndTheOtherIsRefused()
    {
        var mq = new MessageQueue("ModeTopic", "broker-a", 0);

        var subscribeFirst = new DefaultLitePullConsumer("GID_LiteModeSubscribe");
        subscribeFirst.Subscribe("ModeTopic", "TagA");
        subscribeFirst.Subscribe("ModeTopic", "TagB");   // the same shape again is allowed
        var afterAssign = Assert.Throws<MQClientException>(() => subscribeFirst.Assign(new[] { mq }));
        Assert.Equal("Subscribe and assign are mutually exclusive.", afterAssign.Message);
        var afterExpression = Assert.Throws<MQClientException>(
            () => subscribeFirst.SetSubExpressionForAssign("ModeTopic", "TagC"));
        Assert.Equal("Subscribe and assign are mutually exclusive.", afterExpression.Message);

        var assignFirst = new DefaultLitePullConsumer("GID_LiteModeAssign");
        assignFirst.Assign(new[] { mq });
        assignFirst.Assign(new[] { mq });                          // the same shape again is allowed
        assignFirst.SetSubExpressionForAssign("ModeTopic", "TagA"); // and so is this assign-setting call
        var afterSubscribe = Assert.Throws<MQClientException>(
            () => assignFirst.Subscribe("ModeTopic", "*"));
        Assert.Equal("Subscribe and assign are mutually exclusive.", afterSubscribe.Message);
    }

    [Fact]
    public void AssignModeHeartbeat_CarriesNoSubscriptionEntries()
    {
        using var cluster = MockCluster.Start(1);
        var mq = new MessageQueue("ModeHeartbeatTopic", MockCluster.BrokerName(0), 0);
        var c = new DefaultLitePullConsumer("GID_LiteHeartbeatAssign");
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName("lite-hb-assign");
        // Even with an assign expression recorded, the heartbeat must stay subscription-free.
        c.SetSubExpressionForAssign(mq.Topic, "TagA");
        c.Assign(new[] { mq });
        c.Start();
        try
        {
            Assert.True(WaitFor(() => LastHeartbeatFor(cluster, c.ConsumerGroup) is not null),
                "the assigned consumer sends a heartbeat");
            HeartbeatData heartbeat = LastHeartbeatFor(cluster, c.ConsumerGroup)!;
            ConsumerData data = Assert.Single(heartbeat.ConsumerDataSet);
            Assert.Equal(c.ConsumerGroup, data.GroupName);
            Assert.Empty(data.SubscriptionDataSet);
        }
        finally
        {
            c.Shutdown();
        }
    }

    [Fact]
    public void Seek_JudgeMembershipBeforeRunning_AndUseTheModeSpecificMessage()
    {
        var mq = new MessageQueue("SeekTopic", "broker-a", 0);
        var stranger = new MessageQueue("SeekTopic", "broker-a", 7);

        var subscribeMode = new DefaultLitePullConsumer("GID_LiteSeekSubscribe");
        subscribeMode.Subscribe("SeekTopic", "*");
        // Not running at all, yet the membership verdict comes first.
        var subscribeError = Assert.Throws<MQClientException>(() => subscribeMode.Seek(stranger, 0));
        Assert.Equal("The message queue is not in assigned list, may be rebalancing, message queue: "
                     + stranger, subscribeError.Message);

        var assignMode = new DefaultLitePullConsumer("GID_LiteSeekAssign");
        assignMode.Assign(new[] { mq });
        var assignError = Assert.Throws<MQClientException>(() => assignMode.Seek(stranger, 0));
        Assert.Equal("The message queue is not in assigned list, message queue: " + stranger,
            assignError.Message);

        // The queue is held, so the running guard is the next one.
        var notRunning = Assert.Throws<MQClientException>(() => assignMode.Seek(mq, 0));
        Assert.Equal("The consumer not running, please start it first.", notRunning.Message);
    }

    [Fact]
    public void Seek_OutOfRangeReportsAllThreeNumbers_AndLeavesTheCursorUntouched()
    {
        var mq = new MessageQueue("SeekRangeTopic", "broker-a", 0);
        var c = new DefaultLitePullConsumer("GID_LiteSeekRange");
        c.Assign(new[] { mq });
        c.MarkRunningForTest();
        c.SetSeekBoundsForTest(10, 20);

        var below = Assert.Throws<MQClientException>(() => c.Seek(mq, 5));
        Assert.Equal("Seek offset illegal, seek offset = 5, min offset = 10, max offset = 20",
            below.Message);
        Eq(-1L, c.PullCursorOf(mq), "a refused seek pins nothing");

        c.Seek(mq, 15);
        Eq(15L, c.PullCursorOf(mq), "the accepted seek moves the pull cursor");

        var above = Assert.Throws<MQClientException>(() => c.Seek(mq, 21));
        Assert.Equal("Seek offset illegal, seek offset = 21, min offset = 10, max offset = 20",
            above.Message);
        Eq(15L, c.PullCursorOf(mq), "a refused seek leaves the pull cursor untouched");
        Eq(15L, c.ConsumeCursorOf(mq), "and the consume cursor too");
    }

    [Fact]
    public void Seek_BoundaryReadFailureSurfacesAnError_AndDoesNotPinMinusOne()
    {
        var mq = new MessageQueue("SeekFailureTopic", "broker-a", 0);
        var c = new DefaultLitePullConsumer("GID_LiteSeekFailure");
        c.Assign(new[] { mq });
        c.MarkRunningForTest();

        // No client and no test bounds: the boundary read cannot complete.
        Assert.Throws<MQClientException>(() => c.Seek(mq, 0));
        Eq(-1L, c.PullCursorOf(mq), "a failed boundary read must not pin -1 as a seek offset");
    }

    [Fact]
    public void Committed_ThrowsOnFailedBrokerRead_WhileQueryNotFoundStaysMinusOne()
    {
        var mq = new MessageQueue("CommitTopic", MockCluster.BrokerName(0), 0);

        using (var failing = MockCluster.Start(1))
        {
            failing.SetReplyCode(RequestCode.QueryConsumerOffset, ResponseCode.SystemError,
                "offset store down");
            var c = new DefaultLitePullConsumer("GID_LiteCommittedFail");
            c.SetNamesrvAddr(failing.NamesrvAddr);
            c.SetInstanceName("lite-committed-fail");
            c.SetAutoCommit(false);
            c.Assign(new[] { mq });
            c.Start();
            try
            {
                var ex = Assert.Throws<MQClientException>(() => c.Committed(mq));
                Assert.Equal("Fetch consume offset from broker exception", ex.Message);
            }
            finally
            {
                c.Shutdown();
            }
        }

        using (var missing = MockCluster.Start(1))
        {
            missing.SetReplyCode(RequestCode.QueryConsumerOffset, ResponseCode.QueryNotFound);
            var c = new DefaultLitePullConsumer("GID_LiteCommittedMissing");
            c.SetNamesrvAddr(missing.NamesrvAddr);
            c.SetInstanceName("lite-committed-missing");
            c.SetAutoCommit(false);
            c.Assign(new[] { mq });
            c.Start();
            try
            {
                Eq(-1L, c.Committed(mq), "QUERY_NOT_FOUND is the only answer that means -1");
            }
            finally
            {
                c.Shutdown();
            }
        }
    }

    [Fact]
    public void AssignNarrowing_DropsPausedParkedCursorsSeekAndBufferedState()
    {
        var q0 = new MessageQueue("ShrinkTopic", "broker-a", 0);
        var q1 = new MessageQueue("ShrinkTopic", "broker-a", 1);
        var c = new DefaultLitePullConsumer("GID_LiteShrink");
        c.Assign(new[] { q0, q1 });
        c.SetAutoCommit(false);
        c.MarkRunningForTest();
        c.SetSeekBoundsForTest(0, 100);
        c.PullThresholdForAll = int.MaxValue;
        c.PullThresholdForQueue = 1;
        c.PullThresholdSizeForQueue = int.MaxValue;
        c.ConsumeMaxSpan = long.MaxValue;

        c.Seek(q0, 10);
        c.AppendToLocalBufferForTest(new[] { Buffered(q0, 10), Buffered(q0, 11) });
        c.Pause(new[] { q0 });
        Assert.True(c.CacheFlowControlHitForTest(q0), "the cached rows park the queue");
        Assert.True(c.PullParkRemainingMillis(q0) > 0, "the queue really holds a park deadline");

        c.Assign(new[] { q1 });

        Eq(-1L, c.PullCursorOf(q0), "the pull cursor went with the revoked queue");
        Eq(-1L, c.ConsumeCursorOf(q0), "and the consume cursor too");
        Assert.False(c.IsPausedForTest(q0), "the paused flag went with the queue");
        Eq(DefaultLitePullConsumer.NoPullPause, c.LastPullPauseMillis(q0),
            "the parked deadline went with the queue");
        Eq(0, c.CachedMessageCountOf(q0), "the buffered rows went with the queue");
        Eq(0, c.PendingPullRoundsOf(q0), "and its cache accumulator too");
        Eq(0, c.CachedMessageCount, "the shared buffer holds nothing of it any more");

        // Coming back must start from a clean state: the old seek pin is not resurrected.
        c.Assign(new[] { q0, q1 });
        Eq(-1L, c.PullCursorOf(q0), "the revoked seek offset did not come back with the queue");
    }

    [Fact]
    public void Unsubscribe_DropsTheTopicsAssignedQueuesWithoutWaitingForRebalance()
    {
        using var cluster = MockCluster.Start(1);
        var c = new DefaultLitePullConsumer("GID_LiteUnsubscribe");
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName("lite-unsubscribe");
        c.SetAutoCommit(false);
        c.Subscribe("UnsubTopic", "*");
        c.Start();
        try
        {
            Assert.True(WaitFor(() => c.Assignment().Count > 0, 8000),
                "the subscribe-mode rebalance assigned the topic's queue");
            MessageQueue mq = c.Assignment()[0];
            c.Pause(new[] { mq });
            Assert.True(c.IsPausedForTest(mq));

            c.Unsubscribe("UnsubTopic");

            Assert.Empty(c.Assignment());
            Assert.False(c.IsPausedForTest(mq), "the paused flag was dropped immediately");
            Eq(-1L, c.PullCursorOf(mq), "the cursors were dropped immediately");

            // The subscription table is gone too, so the next rebalance cannot hand the
            // topic back (the loop runs every second).
            Thread.Sleep(1200);
            Assert.Empty(c.Assignment());
        }
        finally
        {
            c.Shutdown();
        }
    }

    [Fact]
    public void AssignModeLitePull_CarriesTheAssignExpressionWithTagVersionZero()
    {
        using var cluster = MockCluster.Start(1);
        var mq = new MessageQueue("LiteTagVersionTopic", MockCluster.BrokerName(0), 0);
        var c = new DefaultLitePullConsumer("GID_LiteTagVersion");
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName("lite-tag-version");
        c.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        c.SetAutoCommit(false);
        c.SetSubExpressionForAssign(mq.Topic, "TagA");
        c.Assign(new[] { mq });
        c.Start();
        try
        {
            Assert.True(WaitFor(() => cluster.CountRequests(RequestCode.LitePullMessage) > 0),
                "the assigned queue is pulled");
            WireRecord pull = cluster.Records().Last(r => r.Code == RequestCode.LitePullMessage);
            Assert.Equal(ExpressionType.TAG, pull.Ext["expressionType"]);
            Assert.Equal("TagA", pull.Ext["subscription"]);
            Assert.Equal("0", pull.Ext["subVersion"]);
        }
        finally
        {
            c.Shutdown();
        }
    }

    [Fact]
    public void LitePullRequest_CarriesTheConfiguredSuspendBudget()
    {
        using var cluster = MockCluster.Start(1);
        var mq = new MessageQueue("LiteSuspendTopic", MockCluster.BrokerName(0), 0);
        var c = new DefaultLitePullConsumer("GID_LiteSuspend");
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName("lite-suspend");
        c.SetBrokerSuspendMaxTimeMillis(7000);
        c.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        c.SetAutoCommit(false);
        c.SetSubExpressionForAssign(mq.Topic, "TagA");
        c.Assign(new[] { mq });
        c.Start();
        try
        {
            Assert.True(WaitFor(() => cluster.CountRequests(RequestCode.LitePullMessage) > 0),
                "the assigned queue is pulled");
            WireRecord pull = cluster.Records().Last(r => r.Code == RequestCode.LitePullMessage);
            // The poll stays short because this loop must answer its caller; the field still
            // reports the budget the consumer was configured with, not a constant.
            Assert.Equal("7000", pull.Ext["suspendTimeoutMillis"]);
            Assert.Equal(0, int.Parse(pull.Ext["sysFlag"]) & PullSysFlag.FlagSuspend);
        }
        finally
        {
            c.Shutdown();
        }
    }

    [Fact]
    public void Heartbeat_CarriesTheBuiltSubscriptionSetBeyondRebalanceSubscriptions()
    {
        using var cluster = MockCluster.Start(1);
        var mq = new MessageQueue("LiteBuiltHbTopic", MockCluster.BrokerName(0), 0);
        var c = new DefaultLitePullConsumer("GID_LiteBuiltHb");
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName("lite-built-hb");
        c.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        c.SetAutoCommit(false);
        c.SetSubExpressionForAssign(mq.Topic, "AssignOnly");
        c.Assign(new[] { mq });
        c.BuildSubscriptionsForHeartbeat(new Dictionary<string, MessageSelector>
        {
            ["LiteHelpTag"] = MessageSelector.ByTag("TagA"),
            ["LiteHelpSql"] = MessageSelector.BySql("a > 1"),
        });
        c.Start();
        try
        {
            Assert.True(WaitFor(() => LastHeartbeatFor(cluster, c.ConsumerGroup) is not null),
                "the assign-mode heartbeat reached the broker");
            Dictionary<string, SubscriptionData> subs = LastHeartbeatFor(cluster, c.ConsumerGroup)!
                .ConsumerDataSet.SelectMany(d => d.SubscriptionDataSet)
                .ToDictionary(s => s.Topic, StringComparer.Ordinal);
            Assert.Equal(new[] { "LiteHelpSql", "LiteHelpTag" },
                subs.Keys.OrderBy(k => k, StringComparer.Ordinal));
            Assert.Equal(ExpressionType.TAG, subs["LiteHelpTag"].ExpressionType);
            Assert.Equal("TagA", subs["LiteHelpTag"].SubString);
            Assert.Equal(ExpressionType.Sql92, subs["LiteHelpSql"].ExpressionType);
            Assert.Equal("a > 1", subs["LiteHelpSql"].SubString);
            Assert.DoesNotContain(mq.Topic, subs.Keys);

            // A rebuild replaces the whole set: the topic left out stops being announced.
            c.BuildSubscriptionsForHeartbeat(new Dictionary<string, MessageSelector>
            {
                ["LiteHelpTag"] = MessageSelector.ByTag("*"),
            });
            Assert.Single(c.GetSubscriptionsForHeartbeat());
            SendHeartbeatNow(c);
            subs = LastHeartbeatFor(cluster, c.ConsumerGroup)!
                .ConsumerDataSet.SelectMany(d => d.SubscriptionDataSet)
                .ToDictionary(s => s.Topic, StringComparer.Ordinal);
            Assert.Equal(new[] { "LiteHelpTag" }, subs.Keys);
            Assert.Equal("*", subs["LiteHelpTag"].SubString);

            // A rejected selector leaves the previous set intact; a null map clears it.
            Assert.Throws<ArgumentException>(() =>
                c.BuildSubscriptionsForHeartbeat(new Dictionary<string, MessageSelector>
                {
                    ["LiteBadSql"] = new MessageSelector { Type = ExpressionType.Sql92, Expression = "" },
                }));
            Assert.Equal("LiteHelpTag", Assert.Single(c.GetSubscriptionsForHeartbeat()).Topic);
            Assert.Throws<ArgumentException>(() =>
                c.BuildSubscriptionsForHeartbeat(new Dictionary<string, MessageSelector>
                {
                    [""] = MessageSelector.ByTag("TagA"),
                }));
            Assert.Equal("LiteHelpTag", Assert.Single(c.GetSubscriptionsForHeartbeat()).Topic);
            c.BuildSubscriptionsForHeartbeat(null);
            Assert.Empty(c.GetSubscriptionsForHeartbeat());
        }
        finally
        {
            c.Shutdown();
        }
    }

    [Fact]
    public void BuiltSubscriptionSet_IsNamespacedAndSorted()
    {
        var c = new DefaultLitePullConsumer("GID_LiteBuiltNamespace");
        c.SetNamespace("NSX");
        c.BuildSubscriptionsForHeartbeat(new Dictionary<string, MessageSelector>
        {
            ["TopicB"] = MessageSelector.ByTag("TagA"),
            ["TopicA"] = MessageSelector.ByTag("TagB"),
        });
        Assert.Equal(new[] { "NSX%TopicA", "NSX%TopicB" },
            c.GetSubscriptionsForHeartbeat().Select(s => s.Topic));

        c.BuildSubscriptionsForHeartbeat(new Dictionary<string, MessageSelector>
        {
            ["NSX%TopicA"] = MessageSelector.ByTag("TagB"),
        });
        Assert.Equal("NSX%TopicA", Assert.Single(c.GetSubscriptionsForHeartbeat()).Topic);
    }

    [Fact]
    public void BuiltSubscriptionSet_YieldsToTheRebalanceSubscription()
    {
        using var cluster = MockCluster.Start(1);
        const string topic = "LiteBuiltPrecedenceTopic";
        var c = new DefaultLitePullConsumer("GID_LiteBuiltPrecedence");
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName("lite-built-precedence");
        c.Subscribe(topic, "TagA");
        c.BuildSubscriptionsForHeartbeat(new Dictionary<string, MessageSelector>
        {
            [topic] = MessageSelector.ByTag("TagZ"),
        });
        c.Start();
        try
        {
            Assert.True(WaitFor(() => LastHeartbeatFor(cluster, c.ConsumerGroup) is not null));
            SubscriptionData sub = Assert.Single(
                LastHeartbeatFor(cluster, c.ConsumerGroup)!.ConsumerDataSet.SelectMany(d => d.SubscriptionDataSet),
                s => s.Topic == topic);
            Assert.Equal("TagA", sub.SubString);
        }
        finally
        {
            c.Shutdown();
        }
    }

    // ---------------------------------------------------------------- start state machine

    private static string StateMessage(string state) =>
        "The PullConsumer service state not OK, maybe started once, " + state;

    [Fact]
    public void Start_WhileRunning_ReportsRunning()
    {
        using var cluster = MockCluster.Start(1);
        var c = new DefaultLitePullConsumer("GID_LiteStateRunning");
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName("lite-state-running");
        c.Start();
        try
        {
            var ex = Assert.Throws<MQClientException>(c.Start);
            Assert.Equal(StateMessage("RUNNING"), ex.Message);
        }
        finally
        {
            c.Shutdown();
        }
    }

    [Fact]
    public void Start_AfterFailedStart_ReportsStartFailed()
    {
        var c = new DefaultLitePullConsumer("GID_LiteStateFailed");
        Assert.Throws<MQClientException>(c.Start);   // no name server address

        // The state check comes first, so even a repaired config cannot start this instance.
        c.SetNamesrvAddr("127.0.0.1:1");
        var ex = Assert.Throws<MQClientException>(c.Start);
        Assert.Equal(StateMessage("START_FAILED"), ex.Message);
    }

    [Fact]
    public void Start_AfterShutdown_ReportsShutdownAlreadyAndStaysIdempotent()
    {
        var c = new DefaultLitePullConsumer("GID_LiteStateShutdown");
        c.Shutdown();
        c.Shutdown();   // idempotent

        var ex = Assert.Throws<MQClientException>(c.Start);
        Assert.Equal(StateMessage("SHUTDOWN_ALREADY"), ex.Message);
    }

    [Fact]
    public void CheckConfig_ValidatesModelStrategyAndLongPollingBudget_WithoutASubscription()
    {
        new DefaultLitePullConsumer("GID_LiteCheckConfig").CheckConfig();

        var model = new DefaultLitePullConsumer("GID_LiteCheckConfig");
        model.SetMessageModel("UNKNOWN_MODEL");
        Assert.Contains("messageModel", Assert.Throws<MQClientException>(model.CheckConfig).Message);

        var strategy = new DefaultLitePullConsumer("GID_LiteCheckConfig");
        strategy.SetAllocateMessageQueueStrategy(null);
        Assert.Contains("allocateMessageQueueStrategy",
            Assert.Throws<MQClientException>(strategy.CheckConfig).Message);

        var suspend = new DefaultLitePullConsumer("GID_LiteCheckConfig");
        suspend.SetBrokerSuspendMaxTimeMillis(30000);
        suspend.SetConsumerTimeoutMillisWhenSuspend(20000);
        Assert.Contains("consumerTimeoutMillisWhenSuspend",
            Assert.Throws<MQClientException>(suspend.CheckConfig).Message);

        var group = new DefaultLitePullConsumer();
        Assert.Contains("consumerGroup", Assert.Throws<MQClientException>(group.CheckConfig).Message);
    }

    [Fact]
    public void Start_WithoutSubscription_AllowsALateSubscribeThatRefreshesRouteAndHeartbeat()
    {
        using var cluster = MockCluster.Start(1);
        const string topic = "LiteLateSubscribeTopic";
        var c = new DefaultLitePullConsumer("GID_LiteLateSubscribe");
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetInstanceName("lite-late-subscribe");
        c.Start();
        try
        {
            Assert.True(c.IsStarted);
            cluster.ClearRequests();

            c.Subscribe(topic, "*");

            Assert.True(cluster.CountRequestsWith(RequestCode.GetRouteinfoByTopic, "topic", topic) >= 1,
                "the late subscription refreshes its route");
            HeartbeatData? heartbeat = LastHeartbeatFor(cluster, c.ConsumerGroup);
            Assert.NotNull(heartbeat);
            Assert.Contains(heartbeat!.ConsumerDataSet.SelectMany(d => d.SubscriptionDataSet),
                s => s.Topic == topic);
        }
        finally
        {
            c.Shutdown();
        }
    }
}
