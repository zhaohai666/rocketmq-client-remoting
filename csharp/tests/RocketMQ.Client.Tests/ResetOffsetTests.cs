// Why it must be pinned offline: getting this path wrong shows up in exactly three
// **silent** ways --
//
//
// In a short real-machine window the first two at most show up as "after the reset a
// batch of messages reappears" or "it only surfaces after one crash", and the third is
// not visible at all, so every criterion is placed offline; the real machine has a
// separate line of proof (the examples' live-reset-offset subcommand).
//
// The implementation differences on the C# side (see the comments in Consumer.cs): the
// network leg (persist sends to the broker) is not observable offline, so what is
// asserted here is "the new offset really was handed to the persistence path" (the
// ConsumeOffset/HadOffset of that record in RetiredQueuesForTest is exactly the material
// that goes to UpdateConsumerOffset). The **wire fields** of 222 are pinned separately by
// MockCluster packet capture (the isForce key name, see Admin.cs).
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class ResetOffsetTests
{
    private const string Group = "GID_ResetOffsetNetUnit";
    private const string Topic = "ResetOffsetNetUnitTopic";
    private const string Broker = "broker-a";

    private static MessageQueue Queue(int queueId = 0) => new(Topic, Broker, queueId);

    private static MessageExt Ext(long queueOffset, int queueId = 0) => new()
    {
        Topic = Topic,
        BrokerName = Broker,
        QueueId = queueId,
        QueueOffset = queueOffset,
    };

    private static List<MessageExt> Batch(params int[] offsets)
        => offsets.Select(o => Ext(o)).ToList();

    /// <summary>A push consumer that touches no network: allocation, buffering, offsets
    /// and generation are all set up by hand.</summary>
    private sealed class Harness
    {
        public DefaultMQPushConsumer C { get; }
        public List<MessageQueue> Mqs { get; }
        public List<string> Keys { get; }

        public Harness(int queues = 1, long clusterOffset = 7)
        {
            C = new DefaultMQPushConsumer(Group) { ConsumeMessageBatchMaxSize = 2 };
            Mqs = Enumerable.Range(0, queues).Select(i => Queue(i)).ToList();
            Keys = Mqs.Select(DefaultMQPushConsumer.OffsetKeyForTest).ToList();
            for (int i = 0; i < Mqs.Count; i++)
            {
                C.SetAssignedForTest(Keys[i], Mqs[i]);
                C.RegisterLoopForTest(Keys[i], alive: false);
                C.SetConsumeOffsetForTest(Keys[i], clusterOffset);
            }
        }

        public MessageQueue Mq => Mqs[0];
        public string Key => Keys[0];

        /// <summary>Simulates the queue table pushed by 220: only the queues of the topic
        /// that are in the table get reset.</summary>
        public void Reset(params long[] offsets)
        {
            var table = new Dictionary<MessageQueue, long>();
            for (int i = 0; i < offsets.Length && i < Mqs.Count; i++)
            {
                table[Mqs[i]] = offsets[i];
            }

            C.ResetOffset(Topic, table);
        }

        public long Epoch(int index = 0) => C.QueueEpochForTest(Keys[index]);

        public long? Offset(int index = 0) => C.ConsumeOffsetForTest(Keys[index]);

        public void Ack(int[] offsets, long epoch, int index = 0)
            => C.AdvanceConsumeOffsetForTest(Keys[index],
                offsets.Select(o => Ext(o, Mqs[index].QueueId)).ToList(), null, epoch);

        /// <summary>The next rebalance after a reset (the real path: DoRebalance ->
        /// RebalancePullThreads).</summary>
        public void Rebuild()
        {
            C.SetStartedForTest(true);
            C.RebuildPullThreadsForTest(Mqs);
        }
    }

    // ------------------------------------------------- Withdrawing a queue: in-flight and buffer are invalidated together

    [Fact]
    public void Reset_DropsEverythingForTheQueue()
    {
        var h = new Harness();
        h.C.SetPendingForTest(h.Key, Batch(1, 2));
        h.C.RegisterPopQueueForTest(h.Key);

        h.Reset(3);

        Assert.Empty(h.C.PendingForTest(h.Key));
        Assert.Null(h.Offset());
        Assert.False(h.C.HasPullLoop(h.Key));
        Assert.Equal(-1L, h.C.LastPullAt(h.Key));
        Assert.False(h.C.MqMapContainsForTest(h.Key));
        Assert.Null(h.C.PopQueueForTest(h.Key));
    }

    [Fact]
    public void Reset_BumpsTheQueueEpoch()
    {
        var h = new Harness();
        Assert.Equal(0L, h.Epoch());

        h.Reset(3);

        Assert.Equal(1L, h.Epoch());
    }

    [Fact]
    public void Reset_HandsTheNewOffsetToPersist()
    {
        var h = new Harness();
        h.C.SetPendingForTest(h.Key, Batch(1, 2));

        h.Reset(3);

        DefaultMQPushConsumer.RetiredForTest record = Assert.Single(h.C.RetiredQueuesForTest());
        Assert.True(record.HadOffset);
        Assert.Equal(3L, record.ConsumeOffset);
        Assert.Equal(h.Mq, record.Mq);
    }

    /// <summary>An in-flight batch got the pre-reset generation (0): its ack must be
    /// invalidated as a whole. Before the fix this wrote back 8 -- the offset was judged
    /// "reset and then rolled back to the old position", so the reset only survived until
    /// the next cycle hit the disk.</summary>
    [Fact]
    public void StaleEpochAck_CannotUndoTheReset()
    {
        var h = new Harness();
        h.Reset(3);

        h.Ack(new[] { 7 }, epoch: 0);

        Assert.Null(h.Offset());
    }

    /// <summary>Negative control: this does not permanently freeze the queue -- the
    /// rebuilt queue gets a new generation and acking works normally again.</summary>
    [Fact]
    public void AckAfterRebuild_StillWorks()
    {
        var h = new Harness();
        h.Reset(3);

        h.Rebuild();

        Assert.True(h.C.MqMapContainsForTest(h.Key));
        Assert.Equal(1L, h.Epoch());
        h.Ack(new[] { 3, 4 }, epoch: 1);
        Assert.Equal(5L, h.Offset());
    }

    /// <summary>Negative control: a queue that is not in the 220 queue table (even under
    /// the same topic) must not be touched at all.</summary>
    [Fact]
    public void Reset_OnlyTouchesQueuesInTheTable()
    {
        var h = new Harness(queues: 2);
        h.C.SetPendingForTest(h.Keys[1], Batch(1));

        h.Reset(3);   // only queue-0

        // queue-0: withdrawn (the offset goes to the persistence path on withdrawal and no longer stays in the table)
        Assert.False(h.C.MqMapContainsForTest(h.Keys[0]));
        Assert.Null(h.Offset(0));
        DefaultMQPushConsumer.RetiredForTest record = Assert.Single(h.C.RetiredQueuesForTest());
        Assert.Equal(h.Mqs[0], record.Mq);
        Assert.Equal(3L, record.ConsumeOffset);

        // queue-1: not touched at all
        Assert.True(h.C.MqMapContainsForTest(h.Keys[1]));
        Assert.Equal(7L, h.Offset(1));
        Assert.Single(h.C.PendingForTest(h.Keys[1]));
        Assert.Equal(0L, h.Epoch(1));
    }

    [Fact]
    public void Reset_WithUnknownTopicIsANoop()
    {
        var h = new Harness();

        h.C.ResetOffset("NoSuchTopic", new Dictionary<MessageQueue, long> { [h.Mq] = 0 });

        Assert.Empty(h.C.RetiredQueuesForTest());
        Assert.True(h.C.MqMapContainsForTest(h.Key));
        Assert.Equal(7L, h.Offset());
        Assert.Equal(0L, h.Epoch());
    }

    [Fact]
    public void Reset_WithEmptyTableIsANoop()
    {
        var h = new Harness();

        h.C.ResetOffset(Topic, new Dictionary<MessageQueue, long>());

        Assert.Empty(h.C.RetiredQueuesForTest());
        Assert.True(h.C.MqMapContainsForTest(h.Key));
        Assert.Equal(0L, h.Epoch());
    }

    // ------------------------------------------------- Inbound 220: shape, group, oneway

    private static RemotingCommand ResetCmd(string group, string topic, byte[]? body)
    {
        var cmd = new RemotingCommand { Code = RequestCode.ResetConsumerClientOffset };
        cmd.ExtFields["group"] = group;
        cmd.ExtFields["topic"] = topic;
        cmd.ExtFields["timestamp"] = "-1";
        cmd.ExtFields["isForce"] = "true";
        if (body is not null)
        {
            cmd.Body = body;
            cmd.HasBody = true;
        }

        return cmd;
    }

    /// <summary>Waits for a background thread to finish (220 is oneway, and the reset
    /// logic runs offline on ResetOffsetThread).</summary>
    private static bool WaitFor(Func<bool> done, int timeoutMs = 3000)
    {
        var sw = Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < timeoutMs)
        {
            if (done())
            {
                return true;
            }

            System.Threading.Thread.Sleep(10);
        }

        return done();
    }

    [Fact]
    public void ProcessReset_IsOnewayAndAppliesTheMapBody()
    {
        var h = new Harness();
        var body = new ResetOffsetBody();
        body.OffsetTable[h.Mq] = 5;

        RemotingCommand? resp = h.C.ProcessResetConsumerOffset(
            ResetCmd(Group, Topic, body.Encode()));

        Assert.Null(resp);
        Assert.True(WaitFor(() => h.C.RetiredQueuesForTest().Count == 1),
            "a map-shaped 220 did not reach the reset path");
        DefaultMQPushConsumer.RetiredForTest record = Assert.Single(h.C.RetiredQueuesForTest());
        Assert.Equal(5L, record.ConsumeOffset);
    }

    /// <summary>
    /// When the originator's language=CPP the broker pushes the **array** shape
    /// (ResetOffsetBodyForC): it must be parsed.
    [Fact]
    public void ProcessReset_ArrayBodyForCIsNotDropped()
    {
        var h = new Harness();
        var body = new ResetOffsetBodyForC();
        body.OffsetTable.Add(new MessageQueueForC
        {
            Topic = Topic,
            BrokerName = Broker,
            QueueId = 0,
            Offset = 9,
        });

        Assert.Null(h.C.ProcessResetConsumerOffset(ResetCmd(Group, Topic, body.Encode())));

        Assert.True(WaitFor(() => h.C.RetiredQueuesForTest().Count == 1),
            "the array-shaped 220 was silently dropped");
        DefaultMQPushConsumer.RetiredForTest record = Assert.Single(h.C.RetiredQueuesForTest());
        Assert.Equal(9L, record.ConsumeOffset);
        Assert.Equal(h.Mq, record.Mq);
    }

    /// <summary>A group mismatch (another consumer group in the same process) must not
    /// touch this consumer's offsets.</summary>
    [Fact]
    public void ProcessReset_WrongGroupIsIgnored()
    {
        var h = new Harness();
        var body = new ResetOffsetBody();
        body.OffsetTable[h.Mq] = 5;

        Assert.Null(h.C.ProcessResetConsumerOffset(ResetCmd("GID_SomeoneElse", Topic, body.Encode())));
        System.Threading.Thread.Sleep(300);

        Assert.Empty(h.C.RetiredQueuesForTest());
        Assert.True(h.C.MqMapContainsForTest(h.Key));
        Assert.Equal(7L, h.Offset());
    }

    [Fact]
    public void ProcessReset_GarbageBodyIsIgnored()
    {
        var h = new Harness();

        Assert.Null(h.C.ProcessResetConsumerOffset(
            ResetCmd(Group, Topic, System.Text.Encoding.UTF8.GetBytes("not json at all"))));
        System.Threading.Thread.Sleep(300);

        Assert.Empty(h.C.RetiredQueuesForTest());
        Assert.Equal(7L, h.Offset());
        Assert.Equal(0L, h.Epoch());
    }

    // ------------------------------------------------- The 222 wire: the isForce key name + the default language

    private static DefaultMQAdminExt StartedAdmin(MockCluster cluster)
    {
        var admin = new DefaultMQAdminExt("ADMIN_reset_offset");
        admin.SetNamesrvAddr(cluster.NamesrvAddr);
        admin.Start();
        return admin;
    }

    /// <summary>
    /// 222's force flag is called **isForce** in extFields, not force.
    [Fact]
    public void AdminReset_SendsTheLegacyIsForceKey()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = StartedAdmin(cluster);
        try
        {
            cluster.SetReplyBody(RequestCode.InvokeBrokerToResetOffset, req =>
            {
                var body = new ResetOffsetBody();
                body.OffsetTable[new MessageQueue("T_Reset222", MockCluster.BrokerName(0), 0)] = 10;
                return body.Encode();
            });

            SortedDictionary<MessageQueue, long> offsets =
                admin.ResetOffsetByTimestamp("T_Reset222", "GID_222", timestamp: -1, isForce: true);

            WireRecord hit = Assert.Single(cluster.Records().FindAll(
                r => r.Code == RequestCode.InvokeBrokerToResetOffset));
            Assert.Equal("true", hit.Ext["isForce"]);
            Assert.False(hit.Ext.ContainsKey("force"));   // a wrong key name means constantly false on the broker side
            Assert.Equal("-1", hit.Ext["timestamp"]);     // the "move everything" semantics rely on timestamp=-1
            Assert.Equal("T_Reset222", hit.Ext["topic"]);
            Assert.Equal("GID_222", hit.Ext["group"]);
            Assert.False(hit.Ext.ContainsKey("queueId")); // the timestamp-based overload carries no queueId
            Assert.Equal(10L, offsets.Values.Single());
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void AdminResetByQueueId_KeepsTheIsForceKeyAndAddsTheQueue()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = StartedAdmin(cluster);
        try
        {
            cluster.SetReplyBody(RequestCode.InvokeBrokerToResetOffset, req =>
            {
                var body = new ResetOffsetBody();
                body.OffsetTable[new MessageQueue("T_Reset222", MockCluster.BrokerName(0), 3)] = 7;
                return body.Encode();
            });

            admin.ResetOffsetByQueueId(cluster.BrokerAddrs[0], "GID_222", "T_Reset222", 3, 7);

            WireRecord hit = Assert.Single(cluster.Records().FindAll(
                r => r.Code == RequestCode.InvokeBrokerToResetOffset));
            Assert.Equal("false", hit.Ext["isForce"]);
            Assert.Equal("0", hit.Ext["timestamp"]);
            Assert.Equal("3", hit.Ext["queueId"]);
            Assert.Equal("7", hit.Ext["offset"]);
        }
        finally
        {
            admin.Shutdown();
        }
    }

    [Fact]
    public void AdminReset_KeepsTheClientsOwnLanguage()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQAdminExt admin = StartedAdmin(cluster);
        try
        {
            cluster.SetReplyBody(RequestCode.InvokeBrokerToResetOffset, req =>
            {
                var body = new ResetOffsetBody();
                body.OffsetTable[new MessageQueue("T_Reset222", MockCluster.BrokerName(0), 0)] = 10;
                return body.Encode();
            });

            admin.ResetOffsetByTimestamp("T_Reset222", "GID_222", timestamp: -1);

            WireRecord hit = Assert.Single(cluster.Records().FindAll(
                r => r.Code == RequestCode.InvokeBrokerToResetOffset));
            Assert.Equal(LanguageCode.Dotnet, hit.Language);
            Assert.NotEqual(LanguageCode.Cpp, hit.Language);
        }
        finally
        {
            admin.Shutdown();
        }
    }
}
