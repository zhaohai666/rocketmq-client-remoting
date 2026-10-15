// Offline unit tests for MQClientManager reusing an MQClientInstance across facades --
// using MockCluster, no dependency on a real cluster.
//
//
// The reason for using MockCluster is the same as in SendRetryTests: every message of
// start/shutdown (routes, heartbeats, the 35 unregister) can go through an in-process
// fake cluster, so the life and death of the reference count can be asserted
// synchronously without waiting for any timeout.
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using System.Globalization;
using System.Text;

using Xunit;

namespace RocketMQ.Client.Tests;

public class MQClientManagerTests
{
    private const string Topic = "MgrShareUnitTopic";

    /// <summary>A listener that does nothing: this group of cases only cares about the
    /// life of the instance, not about messages.</summary>
    private sealed class NoopListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context) => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }

    private static DefaultMQProducer NewProducer(MockCluster cluster, string instanceName, string group)
    {
        return new DefaultMQProducer(group)
        {
            NamesrvAddr = cluster.NamesrvAddr,
            InstanceName = instanceName,
        };
    }

    private static DefaultMQPushConsumer NewConsumer(MockCluster cluster, string instanceName, string group)
    {
        var consumer = new DefaultMQPushConsumer(group)
        {
            InstanceName = instanceName,
        };
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.Subscribe(Topic, "*");
        consumer.SetMessageListener(new NoopListener());
        return consumer;
    }

    // ---------------- 1: two facades with the same clientId share one instance ----------------

    [Fact]
    public void SameClientIdFacadesShareOneInstanceWithRefcountTwo()
    {
        using MockCluster cluster = MockCluster.Start(1);
        DefaultMQProducer producer = NewProducer(cluster, "mgr-share-one", "PID_mgr_share_p");
        DefaultMQPushConsumer consumer = NewConsumer(cluster, "mgr-share-one", "GID_mgr_share_c");
        try
        {
            producer.Start();
            consumer.Start();

            // the clientId rule agrees (ip@instanceName) => the same instance, reference count 2
            Assert.Equal(producer.ClientId, consumer.ClientId);
            Assert.Same(producer.Client(), consumer.Client());
            Assert.Equal(2, MQClientManager.Instance.RefCountOf(producer.ClientId));
            Assert.True(MQClientManager.Instance.TryGetMQClientInstance(producer.ClientId, out _));
        }
        finally
        {
            producer.Shutdown();
            consumer.Shutdown();
        }
    }

    // ---------------- 2: shut one down first, the instance is still alive ----------------

    [Fact]
    public void FirstFacadeShutdownKeepsSharedInstanceAlive()
    {
        using MockCluster cluster = MockCluster.Start(1);
        DefaultMQProducer producer = NewProducer(cluster, "mgr-share-alive", "PID_mgr_alive_p");
        DefaultMQPushConsumer consumer = NewConsumer(cluster, "mgr-share-alive", "GID_mgr_alive_c");
        try
        {
            producer.Start();
            consumer.Start();
            MQClientInstance shared = producer.Client();
            Assert.Same(shared, consumer.Client());

            producer.Shutdown();

            Assert.Equal(1, MQClientManager.Instance.RefCountOf(producer.ClientId));
            Assert.True(shared.Started, "shared instance must survive while consumer still uses it");
            Assert.Same(shared, consumer.Client());

            // the surviving instance must really be usable: the consumer-side pull goes over the shared connection and an empty response does not throw
            List<MessageQueue> queues = consumer.FetchSubscribeMessageQueues(Topic);
            Assert.NotEmpty(queues);
        }
        finally
        {
            if (producer.IsStarted)
            {
                producer.Shutdown();
            }
            consumer.Shutdown();
        }
    }

    // ---------------- 3: shutting the consumer down first is the same, the producer can still send ----------------

    [Fact]
    public void ConsumerShutdownFirstDoesNotBreakProducerOnSharedInstance()
    {
        using MockCluster cluster = MockCluster.Start(1);
        DefaultMQProducer producer = NewProducer(cluster, "mgr-share-order", "PID_mgr_order_p");
        DefaultMQPushConsumer consumer = NewConsumer(cluster, "mgr-share-order", "GID_mgr_order_c");
        try
        {
            producer.Start();
            consumer.Start();
            MQClientInstance shared = producer.Client();

            consumer.Shutdown();

            Assert.Equal(1, MQClientManager.Instance.RefCountOf(producer.ClientId));
            Assert.True(shared.Started);
            Assert.Same(shared, producer.Client());

            // "must not bring it down" has to land functionally: the shared instance is still alive, so the send must succeed as usual
            SendResult r = producer.Send(
                new Message(Topic, Encoding.UTF8.GetBytes("after-consumer-shutdown")), 5000);
            Assert.Equal(SendStatus.SendOk, r.SendStatus);
        }
        finally
        {
            producer.Shutdown();
        }
    }

    // ---------------- 4: only after all are shut down does the instance really close ----------------

    [Fact]
    public void LastFacadeShutdownStopsTheInstance()
    {
        using MockCluster cluster = MockCluster.Start(1);
        DefaultMQProducer producer = NewProducer(cluster, "mgr-share-final", "PID_mgr_final_p");
        DefaultMQPushConsumer consumer = NewConsumer(cluster, "mgr-share-final", "GID_mgr_final_c");
        producer.Start();
        consumer.Start();
        MQClientInstance shared = producer.Client();
        string clientId = producer.ClientId;

        producer.Shutdown();
        consumer.Shutdown();

        Assert.Equal(0, MQClientManager.Instance.RefCountOf(clientId));
        Assert.False(MQClientManager.Instance.TryGetMQClientInstance(clientId, out _));
        Assert.False(shared.Started, "instance must be shut down after the last facade leaves");
    }

    // ---------------- 5: different clientIds are independent ----------------

    [Fact]
    public void DifferentClientIdsDoNotShare()
    {
        using MockCluster cluster = MockCluster.Start(1);
        DefaultMQProducer producer = NewProducer(cluster, "mgr-diff-a", "PID_mgr_diff_p");
        DefaultMQPushConsumer consumer = NewConsumer(cluster, "mgr-diff-b", "GID_mgr_diff_c");
        try
        {
            producer.Start();
            consumer.Start();

            Assert.NotEqual(producer.ClientId, consumer.ClientId);
            Assert.NotSame(producer.Client(), consumer.Client());
            Assert.Equal(1, MQClientManager.Instance.RefCountOf(producer.ClientId));
            Assert.Equal(1, MQClientManager.Instance.RefCountOf(consumer.ClientId));

            // shutting one side down only affects its own copy
            producer.Shutdown();
            Assert.Equal(0, MQClientManager.Instance.RefCountOf(producer.ClientId));
            Assert.Equal(1, MQClientManager.Instance.RefCountOf(consumer.ClientId));
            Assert.True(consumer.Client().Started);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    // ---------------- 6: reverse request handlers on a shared instance do not displace each other ----------------

    [Fact]
    public void FacadeProcessorsStackOnSharedInstanceInsteadOfOverwriting()
    {
        using MockCluster cluster = MockCluster.Start(1);
        DefaultMQProducer first = NewProducer(cluster, "mgr-share-proc", "PID_mgr_proc_1");
        DefaultMQProducer second = NewProducer(cluster, "mgr-share-proc", "PID_mgr_proc_2");
        DefaultMQPushConsumer consumer = NewConsumer(cluster, "mgr-share-proc", "GID_mgr_proc_c");
        try
        {
            first.Start();
            second.Start();
            consumer.Start();

            // three facades share one instance, the registration count per code equals the number of facades in use, and the first one started is not displaced
            MQClientInstance shared = first.Client();
            Assert.Same(shared, second.Client());
            Assert.Same(shared, consumer.Client());
            Assert.Equal(3, MQClientManager.Instance.RefCountOf(first.ClientId));
            Assert.Equal(2, shared.FacadeProcessorCount(RequestCode.CheckTransactionState));
            Assert.Equal(1, shared.FacadeProcessorCount(RequestCode.GetConsumerRunningInfo));

            // the producer that started first leaves: 39 only drops its own share, the other producer's is still there
            first.Shutdown();
            Assert.Equal(2, MQClientManager.Instance.RefCountOf(first.ClientId));
            Assert.True(shared.Started);
            Assert.Equal(1, shared.FacadeProcessorCount(RequestCode.CheckTransactionState));

            // the consumer leaves as well: 307 goes to zero while 39 (the second producer) remains
            consumer.Shutdown();
            Assert.Equal(1, MQClientManager.Instance.RefCountOf(first.ClientId));
            Assert.True(shared.Started);
            Assert.Equal(0, shared.FacadeProcessorCount(RequestCode.GetConsumerRunningInfo));
            Assert.Equal(1, shared.FacadeProcessorCount(RequestCode.CheckTransactionState));
        }
        finally
        {
            if (second.IsStarted)
            {
                second.Shutdown();
            }
        }
    }
}
