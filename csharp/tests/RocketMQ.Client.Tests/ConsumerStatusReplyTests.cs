// What GET_CONSUMER_STATUS_FROM_CLIENT(221) answers on a shared client instance.
//
// Two rules are pinned here, and both are invisible on a single-consumer setup:
//   * the reply is looked up per group, so a consumer that does not own the asked
//     group must stay silent -- if it answers first (with an error, or with its own
//     empty table) the sibling's real offsets never reach the wire;
//   * a group nobody on this instance owns is still answered SUCCESS with an empty
//     table, because the asking side always decodes the body: a dropped reply reads
//     as an unreachable client instead of "this group consumed nothing".
//
// The sibling codes answer differently on purpose: 307 and 309 report the group
// nobody here hosts as an error naming it, and 220 -- a oneway push -- sends
// nothing back at all.
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class ConsumerStatusReplyTests
{
    private const string Topic = "ConsumerStatusReplyUnitTopic";
    private const string GroupA = "GID_status_reply_a";
    private const string GroupB = "GID_status_reply_b";
    private const long OffsetA = 1011;
    private const long OffsetB = 2042;

    private sealed class NoopListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context) => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }

    private static DefaultMQPushConsumer NewConsumer(MockCluster cluster, string instanceName, string group)
    {
        var consumer = new DefaultMQPushConsumer(group) { InstanceName = instanceName };
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.Subscribe(Topic, "*");
        consumer.SetMessageListener(new NoopListener());
        return consumer;
    }

    /// <summary>The rendered body entry for one queue: the keys are inline
    /// MessageQueue JSON serialised as an escaped string key.</summary>
    private static string Entry(MessageQueue mq, long offset)
    {
        string key = MessageQueueKeys.MessageQueueKey(mq).Replace("\"", "\\\"");
        return "\"" + key + "\":" + offset;
    }

    private static string Ask(MQClientInstance client, string group, string topic)
    {
        RemotingCommand cmd = RemotingCommand.CreateRequestCommand(RequestCode.GetConsumerStatusFromClient);
        cmd.AddExtField("group", group);
        cmd.AddExtField("topic", topic);
        RemotingCommand? resp = client.DispatchFacadeProcessor(cmd, "127.0.0.1:10911");
        Assert.NotNull(resp);
        Assert.Equal(ResponseCode.Success, resp!.Code);
        return Encoding.UTF8.GetString(resp.Body);
    }

    /// <summary>Ask one broker-initiated code through the shared instance. The
    /// group field is named differently per request, and 220 is a oneway push
    /// that answers nothing.</summary>
    private static RemotingCommand? AskCode(MQClientInstance client, int code, string group,
        string groupKey = "consumerGroup")
    {
        RemotingCommand cmd = RemotingCommand.CreateRequestCommand(code);
        cmd.AddExtField(groupKey, group);
        return client.DispatchFacadeProcessor(cmd, "127.0.0.1:10911");
    }

    /// <summary>Give one consumer a queue with a known consumed offset. Both
    /// consumers get the same queue object with different markers, so a reply
    /// that came from the wrong one is always identifiable.</summary>
    private static void Own(DefaultMQPushConsumer c, MessageQueue mq, long offset)
    {
        string key = DefaultMQPushConsumer.OffsetKeyForTest(mq);
        c.SetAssignedForTest(key, mq);
        c.SetConsumeOffsetForTest(key, offset);
    }

    [Fact]
    public void EachGroupOnASharedInstanceGetsItsOwnOffsetTable()
    {
        using MockCluster cluster = MockCluster.Start(1);
        DefaultMQPushConsumer a = NewConsumer(cluster, "status-reply-share", GroupA);
        DefaultMQPushConsumer b = NewConsumer(cluster, "status-reply-share", GroupB);
        try
        {
            a.Start();
            b.Start();
            // The whole point of the case: one instance, two consumers, so the
            // dispatch really has to choose.
            Assert.Equal(a.ClientId, b.ClientId);
            Assert.Same(a.Client(), b.Client());

            List<MessageQueue> queues = b.FetchSubscribeMessageQueues(Topic);
            Assert.NotEmpty(queues);
            Own(a, queues[0], OffsetA);
            Own(b, queues[0], OffsetB);

            string bodyB = Ask(a.Client(), GroupB, Topic);
            Assert.Contains(Entry(queues[0], OffsetB), bodyB);
            Assert.DoesNotContain(OffsetA.ToString(), bodyB);

            // Control for the assertion above: the sibling's marker does reach the
            // wire for its own group, so "not present" below means routed away and
            // not a reply that never carried offsets at all.
            string bodyA = Ask(a.Client(), GroupA, Topic);
            Assert.Contains(Entry(queues[0], OffsetA), bodyA);
            Assert.DoesNotContain(OffsetB.ToString(), bodyA);
        }
        finally
        {
            a.Shutdown();
            b.Shutdown();
        }
    }

    [Fact]
    public void AGroupNobodyOnTheInstanceOwnsAnswersAnEmptyTable()
    {
        using MockCluster cluster = MockCluster.Start(1);
        DefaultMQPushConsumer a = NewConsumer(cluster, "status-reply-unowned", GroupA);
        try
        {
            a.Start();
            List<MessageQueue> queues = a.FetchSubscribeMessageQueues(Topic);
            Assert.NotEmpty(queues);
            Own(a, queues[0], OffsetA);

            string body = Ask(a.Client(), "GID_status_reply_unhosted", Topic);
            // A whole, decodable GetConsumerStatusBody with nothing in it -- not
            // an error remark, and not this consumer's offsets.
            Assert.Contains("\"messageQueueTable\":{}", body);
            Assert.Contains("\"consumerTable\":{}", body);

            // Control: the same dispatch does report the hosted group's offsets,
            // so the empty reply above is the group lookup and not a handler that
            // never fills a table.
            Assert.Contains(Entry(queues[0], OffsetA), Ask(a.Client(), GroupA, Topic));
        }
        finally
        {
            a.Shutdown();
        }
    }

    /// <summary>The sibling codes need the opposite answer: the initiating admin
    /// reports whichever client it probed, so a group nobody here hosts has to
    /// come back as an error naming that group -- and a consumer that does not
    /// own it must not be the one to produce that error, because it would be the
    /// first answer the instance hands back.</summary>
    [Fact]
    public void AForeignGroupIsAnsweredByTheInstanceAndNotByASibling()
    {
        using MockCluster cluster = MockCluster.Start(1);
        DefaultMQPushConsumer a = NewConsumer(cluster, "running-info-share", GroupA);
        DefaultMQPushConsumer b = NewConsumer(cluster, "running-info-share", GroupB);
        try
        {
            a.Start();
            b.Start();
            Assert.Same(a.Client(), b.Client());

            // Registered first, so its handler is the one consulted before the
            // group's owner: answering here would decide the reply for GroupB.
            RemotingCommand? infoB = AskCode(a.Client(), RequestCode.GetConsumerRunningInfo, GroupB);
            Assert.NotNull(infoB);
            Assert.Equal(ResponseCode.Success, infoB!.Code);

            // Control: the same dispatch does build a running info for a hosted
            // group, so SUCCESS above is the routing and not a silent default.
            RemotingCommand? infoA = AskCode(a.Client(), RequestCode.GetConsumerRunningInfo, GroupA);
            Assert.NotNull(infoA);
            Assert.Equal(ResponseCode.Success, infoA!.Code);

            const string unhosted = "GID_status_reply_unhosted";
            RemotingCommand? missingInfo = AskCode(a.Client(), RequestCode.GetConsumerRunningInfo, unhosted);
            Assert.NotNull(missingInfo);
            Assert.Equal(ResponseCode.SystemError, missingInfo!.Code);
            Assert.Equal("The Consumer Group <" + unhosted + "> not exist in this consumer",
                missingInfo.Remark);

            // 309 would run the listener of whichever consumer replied, so a
            // foreign group must never reach one: the reply has to be the
            // instance's own, naming the group that was actually asked about.
            RemotingCommand? missingDirect = AskCode(a.Client(), RequestCode.ConsumeMessageDirectly, unhosted);
            Assert.NotNull(missingDirect);
            Assert.Equal(ResponseCode.SystemError, missingDirect!.Code);
            Assert.Equal("The Consumer Group <" + unhosted + "> not exist in this consumer",
                missingDirect.Remark);

            // 220 is a oneway push: the consumer applies it and sends nothing back.
            Assert.Null(AskCode(a.Client(), RequestCode.ResetConsumerClientOffset, GroupB, "group"));
        }
        finally
        {
            a.Shutdown();
            b.Shutdown();
        }
    }

    /// <summary>The topic argument of a 221 query only filters when it names a
    /// topic: the asking side leaves it empty to mean "everything this client has
    /// consumed", so an empty string must not read as "match a topic literally
    /// named empty" and report nothing.</summary>
    [Fact]
    public void ABlankTopicInA221QueryReportsTheWholeTable()
    {
        using MockCluster cluster = MockCluster.Start(1);
        DefaultMQPushConsumer a = NewConsumer(cluster, "status-reply-blank", GroupA);
        try
        {
            a.Start();
            List<MessageQueue> queues = a.FetchSubscribeMessageQueues(Topic);
            Assert.NotEmpty(queues);
            MessageQueue other = new MessageQueue("ConsumerStatusReplyOtherTopic",
                queues[0].BrokerName, 0);
            Own(a, queues[0], OffsetA);
            Own(a, other, OffsetB);

            string whole = Ask(a.Client(), GroupA, "");
            Assert.Contains(Entry(queues[0], OffsetA), whole);
            Assert.Contains(Entry(other, OffsetB), whole);

            // Control: a named topic really does filter, so the blank query above
            // is the no-filter rule and not a handler that ignores the field.
            string scoped = Ask(a.Client(), GroupA, Topic);
            Assert.Contains(Entry(queues[0], OffsetA), scoped);
            Assert.DoesNotContain(Entry(other, OffsetB), scoped);
        }
        finally
        {
            a.Shutdown();
        }
    }
}
