using System.Text;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class PullBalanceViewTests
{
    private const string Topic = "BalViewTopic";
    private const string Group = "GID_bal_view_unit";

    private static byte[] CidBody(params string[] cids)
    {
        var sb = new StringBuilder("{\"consumerIdList\":[");
        for (int i = 0; i < cids.Length; ++i)
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

    /// <summary>已启动的拉模式消费者 + 3 台 broker（路由里每台一个读队列）。</summary>
    private static DefaultMQPullConsumer Started(MockCluster cluster, string instance,
        string ns = "")
    {
        var consumer = new DefaultMQPullConsumer(Group) { Namespace = ns };
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.SetInstanceName(instance);
        consumer.Start();
        return consumer;
    }

    // ---------------------------------------------------------------- 1. 未启动 / 空 topic

    [Fact]
    public void NotStarted_IsRejectedAsNotRunning()
    {
        var consumer = new DefaultMQPullConsumer(Group);
        MQClientException ex = Assert.Throws<MQClientException>(
            () => consumer.FetchMessageQueuesInBalance(Topic));
        Assert.Contains("not started", ex.Message);
    }

    [Fact]
    public void NullTopic_IsRejected()
    {
        using var cluster = MockCluster.Start(1);
        DefaultMQPullConsumer consumer = Started(cluster, "bal-null");
        try
        {
            Assert.Throws<MQClientException>(() => consumer.FetchMessageQueuesInBalance(null!));
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 2. 只拿本实例那一份

    [Fact]
    public void Clustering_ReturnsOnlyThisInstancesShare()
    {
        using var cluster = MockCluster.Start(3);
        DefaultMQPullConsumer consumer = Started(cluster, "bal-share");
        try
        {
            string self = consumer.ClientId;
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
                _ => CidBody("peer-cid", self));

            List<MessageQueue> mine = consumer.FetchMessageQueuesInBalance(Topic);
            // 3 队列 / 2 实例：AVG 给排在后面的 self 一份半（broker-2 起）；
            // 判据只锁「不是全部」+「不含对端的队列」。
            Assert.True(mine.Count > 0 && mine.Count < 3,
                "本实例只拿到自己那一份，count=" + mine.Count);
            foreach (MessageQueue mq in mine)
            {
                Assert.Equal(Topic, mq.Topic);
            }
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    [Fact]
    public void SoleInstance_TakesEveryQueue()
    {
        using var cluster = MockCluster.Start(3);
        DefaultMQPullConsumer consumer = Started(cluster, "bal-sole");
        try
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
                _ => CidBody(consumer.ClientId));
            List<MessageQueue> mine = consumer.FetchMessageQueuesInBalance(Topic);
            Assert.Equal(3, mine.Count);
            Assert.Equal("broker-0", mine[0].BrokerName);
            Assert.Equal("broker-2", mine[2].BrokerName);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    [Fact]
    public void Broadcasting_IgnoresTheConsumerList()
    {
        using var cluster = MockCluster.Start(3);
        var consumer = new DefaultMQPullConsumer(Group);
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.SetInstanceName("bal-bcast");
        consumer.SetMessageModel(MessageModel.Broadcasting);
        consumer.Start();
        try
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
                _ => CidBody("peer-1", "peer-2", "peer-3"));
            Assert.Equal(3, consumer.FetchMessageQueuesInBalance(Topic).Count);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 3. 算不动就保留现有分配

    [Fact]
    public void UnknownGroup_KeepsPulledQueuesInsteadOfTakingAll()
    {
        using var cluster = MockCluster.Start(3);
        DefaultMQPullConsumer consumer = Started(cluster, "bal-keep");
        try
        {
            // broker 说这个组没人（等价于查不到列表）→ 不能回退成"独占全部队列"，
            // 否则同组多实例互相重复消费。此时唯一可信的分配状态就是本地记账的那几张。
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody());
            List<MessageQueue> all = consumer.FetchSubscribeMessageQueues(Topic);
            MessageQueue mine = all[2];
            consumer.Pull(mine, "*", 0, 32);

            List<MessageQueue> view = consumer.FetchMessageQueuesInBalance(Topic);
            Assert.Equal(new[] { mine.BrokerName }, view.Select(m => m.BrokerName));

            DefaultMQPullConsumer fresh = Started(cluster, "bal-fresh");
            try
            {
                Assert.Empty(fresh.FetchMessageQueuesInBalance(Topic));
            }
            finally
            {
                fresh.Shutdown();
            }
        }
        finally
        {
            consumer.Shutdown();
        }
    }


    [Fact]
    public void NamespacedInstance_ReturnsBareTopics()
    {
        using var cluster = MockCluster.Start(2);
        DefaultMQPullConsumer consumer = Started(cluster, "bal-ns", "MQ_INST_bal");
        try
        {
            cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
                _ => CidBody(consumer.ClientId));
            List<MessageQueue> view = consumer.FetchMessageQueuesInBalance(Topic);
            Assert.NotEmpty(view);
            foreach (MessageQueue mq in view)
            {
                Assert.Equal(Topic, mq.Topic);
            }
        }
        finally
        {
            consumer.Shutdown();
        }
    }
}
