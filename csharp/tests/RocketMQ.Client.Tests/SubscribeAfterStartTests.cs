// Offline unit tests for subscribe-after-start plus the immediate heartbeat (#73) -- no cluster needed.
//
//
// Why it must be pinned: on a real machine "the new subscription is not pushed to the
// broker" is silent -- it only shows up as QUERY_TOPIC_CONSUME_BY_WHO(300) not finding the
// group and a new topic getting no queues, while the client says nothing at all.
//
// How far offline can reach: the consumer starts against an unreachable name server (no
// route can be fetched => KnownBrokerAddrs is empty => not a single heartbeat can go out),
// so all that can be proven is
//   (1) Subscribe after Start() no longer throws "already started";
//   (2) the new subscription enters the live subscription table right away (SubscribedTopics(),
//       the table the heartbeat and the rebalance read);
//   (3) Unsubscribe still only removes the entry.
// At the wire level, "the broker really received a heartbeat carrying the new subscription"
// is verified on a real machine by the examples' live-subscribe scenario (the code-300 query
// on the broker's topicGroupTable is the only trustworthy observation).
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
    /// <summary>An address that never connects: the route fetch is refused outright (only
    /// a debug log), zero brokers => no packets sent.</summary>
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

            // the old implementation threw MQClientException("consumer already started...") here.
            c.Subscribe("LateTopic", "TagA||TagB");
            Assert.Contains("LateTopic", c.SubscribedTopics());

            c.Subscribe("LateSqlTopic", MessageSelector.BySql("a > 1"));
            Assert.Contains("LateSqlTopic", c.SubscribedTopics());

            // zero brokers: not one heartbeat can go out and the counter does not move (which also proves nothing was sent behind the scenes above).
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
