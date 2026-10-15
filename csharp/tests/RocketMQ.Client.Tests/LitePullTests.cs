// Unit tests for the lightweight pull consumer (DefaultLitePullConsumer) and the
// 220/221/309 bodies (covering the main pull flow and the core offset table scenarios).
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
    public void Subscribe_Assign_Seek_Poll_BeforeStart_DoesNotCrash()
    {
        var c = new DefaultLitePullConsumer();

        c.Subscribe("MyTopic", "TagA || TagB");

        // assign (resolving the initial offset while not started will fail but must be swallowed, it must not crash)
        var mq = new MessageQueue("MyTopic", "broker-a", 3);
        c.Assign(new[] { mq });
        Assert.Single(c.Assignment());
        Assert.Equal(3, c.Assignment()[0].QueueId);

        // committed must return -1 while not started (there is no client to query)
        Assert.Equal(-1, c.Committed(mq));

        // poll with an empty buffer: a short timeout returns an empty list (no blocking, no crash)
        Assert.Empty(c.Poll(20));

        // seek must be able to register an offset while not started too (it does not query the broker)
        c.Seek(mq, 42);
        Assert.Single(c.Assignment());

        // pause / resume must not crash while not started
        c.Pause(new[] { mq });
        c.Resume(new[] { mq });

        c.Subscribe("MyTopic", "TagC");
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
}
