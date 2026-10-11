// MQClientManager 跨门面复用 MQClientInstance 的离线单测 —— 用 MockCluster，不依赖真集群。
//
// 对齐基准（Java 5.5.1，逐行读过）：
//   * `MQClientManager#getOrCreateMQClientInstance`:50-66 —— factoryTable 按 clientId 缓存，
//     同进程内同 clientId 的 producer/consumer/admin 复用同一个 MQClientInstance
//     （连接、路由缓存、路由/动态取址周期任务、统计采样全共享）。
//   * `MQClientInstance#shutdown`:1101-1133 —— consumerTable/adminExtTable 非空或
//     producerTable 还有人在用时**直接 return**，引用归零才真正关停周期任务与连接，
//     关停时才 removeClientFactory(clientId, this)。等价于本端口的引用计数。
//   * shutdown 顺序：先退出的门面不能拖垮还在用的实例（Java 的空表判据保证），
//     全部退出后实例必须真的关停（周期任务/连接不泄漏）。
//
// 用 MockCluster 的原因与 SendRetryTests 相同：start/shutdown 的全部报文（路由、心跳、
// 35 注销）都能在进程内假集群上走通，引用计数的生死可以同步断言，不用等任何超时。
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

    /// <summary>什么都不做的监听器：本组用例只关心实例的生死，不关心消息。</summary>
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

    // ---------------- 1：同 clientId 两门面共享同一实例 ----------------

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

            // clientId 口径一致（ip@instanceName）⇒ 同一实例、引用计数 2
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

    // ---------------- 2：先关一个，实例仍存活 ----------------

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

            // 引用计数 2 → 1：实例不能被先退出的门面关掉（Java shutdown() 的空表判据）
            Assert.Equal(1, MQClientManager.Instance.RefCountOf(producer.ClientId));
            Assert.True(shared.Started, "shared instance must survive while consumer still uses it");
            Assert.Same(shared, consumer.Client());

            // 存活的实例要真的能用：消费者侧的拉取走共享连接，拉空应答不抛
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

    // ---------------- 3：先关消费者也一样，生产者还能发 ----------------

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

            // 「不能拖垮」要落在功能上：共享实例还活着，发送必须照常成功
            SendResult r = producer.Send(
                new Message(Topic, Encoding.UTF8.GetBytes("after-consumer-shutdown")), 5000);
            Assert.Equal(SendStatus.SendOk, r.SendStatus);
        }
        finally
        {
            producer.Shutdown();
        }
    }

    // ---------------- 4：全关后实例才真正关闭 ----------------

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

        // 引用归零：表项摘除 + 实例真关停（对应 Java removeClientFactory + shutdown OK）
        Assert.Equal(0, MQClientManager.Instance.RefCountOf(clientId));
        Assert.False(MQClientManager.Instance.TryGetMQClientInstance(clientId, out _));
        Assert.False(shared.Started, "instance must be shut down after the last facade leaves");
    }

    // ---------------- 5：不同 clientId 各自独立 ----------------

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

            // 一边关闭只影响自己那一份
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

    // ---------------- 6：共享实例上的反向请求处理器不互相顶掉 ----------------
    //
    // remoting 的处理器表一个 code 只有一个槽：两个生产者共享实例时，后启动的若直接
    // RegisterProcessor 会把先启动的 39（CHECK_TRANSACTION_STATE）顶掉。实例级多播
    // （Java 把这些处理器挂在实例级 ClientRemotingProcessor 上按组路由）的落点就是
    // 同 code 的登记人数 —— 用两个同 clientId 的生产者把这个「不覆盖」锁死。

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

            // 三个门面共享同一实例，各 code 的登记数 = 在用门面数，先启动的不被顶掉
            MQClientInstance shared = first.Client();
            Assert.Same(shared, second.Client());
            Assert.Same(shared, consumer.Client());
            Assert.Equal(3, MQClientManager.Instance.RefCountOf(first.ClientId));
            Assert.Equal(2, shared.FacadeProcessorCount(RequestCode.CheckTransactionState));
            Assert.Equal(1, shared.FacadeProcessorCount(RequestCode.GetConsumerRunningInfo));

            // 先启动的生产者退场：39 只摘它自己那份，另一个生产者的还在
            first.Shutdown();
            Assert.Equal(2, MQClientManager.Instance.RefCountOf(first.ClientId));
            Assert.True(shared.Started);
            Assert.Equal(1, shared.FacadeProcessorCount(RequestCode.CheckTransactionState));

            // 消费者也退场：307 归零，39（第二个生产者）仍在
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
