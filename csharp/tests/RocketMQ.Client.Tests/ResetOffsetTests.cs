// 为什么必须离线锁死：这条路径错了全是**静默**的三种表现 ——
//
//
// 真机短期窗口里前两条最多表现为"重置后消息又冒出来一批"或"崩一次才暴露"，第三条根本
// 看不出来，所以判据全部放离线；真机另有链路证明（examples 的 live-reset-offset 子命令）。
//
// C# 侧的实现差异（见 Consumer.cs 注释）：网络段（persist 发给 broker）离线不可观测，
// 此处断言"新位点确实交给了持久化路径"（RetiredQueuesForTest 里那条记录的
// ConsumeOffset/HadOffset 就是会走 UpdateConsumerOffset 的材料）。222 的**报文字段**
// 另由 MockCluster 抓包锁死（isForce 键名，见 Admin.cs）。
//
// 与 python/tests/test_reset_offset_handler.py、cpp/tests/test_reset_offset.cpp 的同名用例逐条对拍。
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

    /// <summary>不碰网络的 push consumer：分配、缓冲、位点、代号都手动搭。</summary>
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

        /// <summary>模拟 220 下发的队列表：topic 内、在表里的队列才被重置。</summary>
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

        /// <summary>重置之后的下一趟 rebalance（真实路径：DoRebalance → RebalancePullThreads）。</summary>
        public void Rebuild()
        {
            C.SetStartedForTest(true);
            C.RebuildPullThreadsForTest(Mqs);
        }
    }

    // ------------------------------------------------- 撤队列：在途/缓冲一起作废

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

    /// <summary>在途批次拿的是重置前的代号（0）：它的 ack 必须整批作废。修好之前这里会写回 8——
    /// 位点被判"重置过又退回旧位置"，重置只活到下一个周期落盘。</summary>
    [Fact]
    public void StaleEpochAck_CannotUndoTheReset()
    {
        var h = new Harness();
        h.Reset(3);

        h.Ack(new[] { 7 }, epoch: 0);

        Assert.Null(h.Offset());
    }

    /// <summary>负向对照：不是把这条队列永久冻死 —— 重建后的队列拿新代号，ack 恢复正常。</summary>
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

    /// <summary>负向对照：不在 220 队列表里的队列（同 topic 也）一根手指都不许碰。</summary>
    [Fact]
    public void Reset_OnlyTouchesQueuesInTheTable()
    {
        var h = new Harness(queues: 2);
        h.C.SetPendingForTest(h.Keys[1], Batch(1));

        h.Reset(3);   // 只带 queue-0

        // queue-0：撤走（位点随撤销交给持久化路径，不再留在表里）
        Assert.False(h.C.MqMapContainsForTest(h.Keys[0]));
        Assert.Null(h.Offset(0));
        DefaultMQPushConsumer.RetiredForTest record = Assert.Single(h.C.RetiredQueuesForTest());
        Assert.Equal(h.Mqs[0], record.Mq);
        Assert.Equal(3L, record.ConsumeOffset);

        // queue-1：一根手指都没碰
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

    // ------------------------------------------------- 入站 220：形状、组、oneway

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

    /// <summary>等一个后台线程跑完（220 是 oneway，重置逻辑离线跑在 ResetOffsetThread 上）。</summary>
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
            "map 形状的 220 没落到重置路径");
        DefaultMQPushConsumer.RetiredForTest record = Assert.Single(h.C.RetiredQueuesForTest());
        Assert.Equal(5L, record.ConsumeOffset);
    }

    /// <summary>
    /// 发起方 language=CPP 时 broker 推**数组**形状（ResetOffsetBodyForC）：必须解析出来。
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
            "数组形状的 220 被静默丢弃了");
        DefaultMQPushConsumer.RetiredForTest record = Assert.Single(h.C.RetiredQueuesForTest());
        Assert.Equal(9L, record.ConsumeOffset);
        Assert.Equal(h.Mq, record.Mq);
    }

    /// <summary>组不匹配（同进程里有别的消费组）不许动本消费者的位点。</summary>
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

    // ------------------------------------------------- 222 报文：isForce 键名 + 默认 language

    private static DefaultMQAdminExt StartedAdmin(MockCluster cluster)
    {
        var admin = new DefaultMQAdminExt("ADMIN_reset_offset");
        admin.SetNamesrvAddr(cluster.NamesrvAddr);
        admin.Start();
        return admin;
    }

    /// <summary>
    /// 222 的 force 标志在 extFields 里叫 **isForce**，不是 force。
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
            Assert.False(hit.Ext.ContainsKey("force"));   // 键名写错就是 broker 侧恒 false
            Assert.Equal("-1", hit.Ext["timestamp"]);     // 前重语义靠 timestamp=-1
            Assert.Equal("T_Reset222", hit.Ext["topic"]);
            Assert.Equal("GID_222", hit.Ext["group"]);
            Assert.False(hit.Ext.ContainsKey("queueId")); // 按 timestamp 的重载不带 queueId
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
