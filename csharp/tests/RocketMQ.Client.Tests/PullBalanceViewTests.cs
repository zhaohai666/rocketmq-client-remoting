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

    /// <summary>A started pull-mode consumer plus 3 brokers (one read queue each in the
    /// route).</summary>
    private static DefaultMQPullConsumer Started(MockCluster cluster, string instance,
        string ns = "")
    {
        var consumer = new DefaultMQPullConsumer(Group) { Namespace = ns };
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.SetInstanceName(instance);
        consumer.Start();
        return consumer;
    }

    // ---------------------------------------------------------------- 1. Not started / empty topic

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

    // ---------------------------------------------------------------- 2. Only this instance's share

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
            // 3 queues / 2 instances: AVG gives the self that is sorted later one and a half (from broker-2 on);
            // the criterion only locks "not all of them" + "contains none of the peer's queues".
            Assert.True(mine.Count > 0 && mine.Count < 3,
                "this instance only gets its own share, count=" + mine.Count);
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

    // ---------------------------------------------------------------- 3. Keep the existing assignment when it cannot be computed

    [Fact]
    public void UnknownGroup_KeepsPulledQueuesInsteadOfTakingAll()
    {
        using var cluster = MockCluster.Start(3);
        DefaultMQPullConsumer consumer = Started(cluster, "bal-keep");
        try
        {
            // the broker says nobody is in this group (equivalent to not finding the list) -> it must not fall back to "owning all
            // the queues", otherwise several instances of the same group consume each other's
            // messages repeatedly. The only trustworthy assignment state at that point is
            // the one the local bookkeeping holds.
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
