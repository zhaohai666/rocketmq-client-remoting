// 订阅后置 + 立即心跳（#73）离线单测 —— 不需要集群。
//
// 对齐基准（Java 5.5.1 DefaultMQPushConsumerImpl）：
//   * subscribe(topic, subExpression):1265-1275（class-filter / MessageSelector 两个重载同理）
//     都是 put 进 subscriptionInner 之后 `if (mQClientFactory != null)
//     mQClientFactory.sendHeartbeatToAllBrokerWithLock();` —— **没有**「started 之后禁止订阅」
//     这道闸门，心跳是同步立即发的。
//   * unsubscribe(topic):1317-1319 只 remove，**不**发心跳。
//
// 为什么必须锁死：真机上「新订阅没推给 broker」是静默的 —— 只表现为
// QUERY_TOPIC_CONSUME_BY_WHO(300) 查不到本组、新 topic 分不到队列，客户端一声不响。
//
// 离线能测到哪一步：消费者对着连不上的 name server 启动（路由拉不到 ⇒ KnownBrokerAddrs
// 为空 ⇒ 心跳一台都发不出去），因此只能证明
//   ① Start() 之后 Subscribe 不再抛 already started；
//   ② 新订阅立刻进活订阅表（SubscribedTopics()，即心跳与 rebalance 读的那张表）；
//   ③ Unsubscribe 照旧只删表项。
// 报文层面「broker 真收到带新订阅的心跳」由 examples 的 live-subscribe 场景在真机验证
// （broker 侧 topicGroupTable 的 300 号查询是唯一可信观测）。
using System;
using System.Collections.Generic;
using System.Linq;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class SubscribeAfterStartTests
{
    /// <summary>永远连不上的地址：路由拉取当场被拒（只记 debug 日志），零 broker ⇒ 不发包。</summary>
    private const string DeadNamesrv = "127.0.0.1:1";

    private sealed class NoopListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context) => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }

    private static DefaultMQPushConsumer StartedConsumer(string group)
    {
        var c = new DefaultMQPushConsumer(group);
        c.SetNamesrvAddr(DeadNamesrv);
        c.Subscribe("BaseTopic");
        c.SetMessageListener(new NoopListener());
        c.Start();
        return c;
    }

    [Fact]
    public void SubscribeAfterStartIsAllowedAndLandsInTheLiveTable()
    {
        DefaultMQPushConsumer c = StartedConsumer("CID_net_sub_after_start");
        try
        {
            Assert.True(c.IsStarted);

            // 旧实现在这里抛 MQClientException("consumer already started...")。
            c.Subscribe("LateTopic", "TagA||TagB");
            Assert.Contains("LateTopic", c.SubscribedTopics());

            // MessageSelector 重载同样允许（Java :1289-1303）。
            c.Subscribe("LateSqlTopic", MessageSelector.BySql("a > 1"));
            Assert.Contains("LateSqlTopic", c.SubscribedTopics());

            // 零 broker：心跳一台都发不出去，计数器不动（同时反证上面没偷偷发包）。
            Assert.Equal(0, c.HeartbeatCount);
        }
        finally
        {
            c.Shutdown();
        }
    }

    [Fact]
    public void UnsubscribeOnlyDropsTheEntry()
    {
        DefaultMQPushConsumer c = StartedConsumer("CID_net_sub_unsub");
        try
        {
            c.Subscribe("LateTopic");
            long before = c.HeartbeatCount;

            c.Unsubscribe("LateTopic");

            Assert.Equal(before, c.HeartbeatCount);
            Assert.DoesNotContain("LateTopic", c.SubscribedTopics());
            Assert.Contains("BaseTopic", c.SubscribedTopics());
        }
        finally
        {
            c.Shutdown();
        }
    }

    [Fact]
    public void SubscribeBeforeStartStillWorks()
    {
        var c = new DefaultMQPushConsumer("CID_net_sub_before_start");
        c.Subscribe("T_A");
        Assert.Contains("T_A", c.SubscribedTopics());
        Assert.Equal(0, c.HeartbeatCount);
    }
}
