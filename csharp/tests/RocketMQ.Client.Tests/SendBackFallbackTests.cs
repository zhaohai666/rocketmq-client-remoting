// Consumer send-back (CONSUMER_SEND_MSG_BACK) contract, pinned on the wire.
//
// A refused bounce -- no master, transport failure, non-success answer -- must not lose the
// redelivery: the client re-sends it as a normal message to %RETRY%<group> through the
// internal producer, carrying the retry bookkeeping (RETRY_TOPIC / RECONSUME_TIME /
// MAX_RECONSUME_TIMES), dropping the half-message marker, and keeping the body and flag.
// Only when that send fails too does the ORIGINAL error reach the caller -- the fallback's
// error must never substitute for it, otherwise the first failure's reason is lost.
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class SendBackFallbackTests
{
    private const string Topic = "SendBackFallbackUnitTopic";
    private const string PushGroup = "GID_sendback_fallback_push";
    private const string PullGroup = "GID_sendback_fallback_pull";
    private const string Broker = "broker-0";

    private sealed class NoopListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context) => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }

    private static MessageExt Ext(int reconsumeTimes = 0, string broker = Broker)
    {
        var m = new MessageExt
        {
            Topic = Topic,
            BrokerName = broker,
            QueueId = 0,
            QueueOffset = 7,
            CommitLogOffset = 4242,
            MsgId = "MSG-ORIGIN-ID",
            Body = Encoding.UTF8.GetBytes("send-back body"),
        };
        m.ReconsumeTimes = reconsumeTimes;
        m.PutProperty(MessageConst.PropertyTransactionPrepared, "true");
        return m;
    }

    private static DefaultMQPushConsumer NewPushConsumer(MockCluster cluster, string instance)
    {
        var consumer = new DefaultMQPushConsumer(PushGroup) { InstanceName = instance };
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        consumer.Subscribe(Topic, "*");
        consumer.SetMessageListener(new NoopListener());
        return consumer;
    }

    private static DefaultMQPullConsumer NewPullConsumer(MockCluster cluster, string instance)
    {
        var consumer = new DefaultMQPullConsumer(PullGroup);
        consumer.SetInstanceName(instance);
        consumer.SetNamesrvAddr(cluster.NamesrvAddr);
        return consumer;
    }

    private static List<WireRecord> Bounces(MockCluster cluster) =>
        cluster.Records().Where(r => r.Code == RequestCode.ConsumerSendMsgBack).ToList();

    private static List<WireRecord> NormalSends(MockCluster cluster) =>
        cluster.Records().Where(r => r.Code == RequestCode.SendMessageV2).ToList();

    /// <summary>The retry copy's wire shape: retry topic, internal producer, bookkeeping
    /// written, half-message marker gone, payload untouched.</summary>
    private static void AssertRetryCopy(WireRecord send, string group, int maxReconsumeTimes,
        int sourceReconsumeTimes, byte[] body)
    {
        Assert.Equal(MixAll.ClientInnerProducerGroup, send.Ext["a"]);
        Assert.Equal(MixAll.GetRetryTopic(group), send.Ext["b"]);
        Assert.Equal(body, send.Body);

        string props = send.Ext["i"];
        Assert.Contains(MessageConst.PropertyRetryTopic + "\u0001" + Topic, props);
        Assert.Contains(MessageConst.PropertyReconsumeTime + "\u0001"
                       + (sourceReconsumeTimes + 1).ToString(), props);
        Assert.Contains(MessageConst.PropertyMaxReconsumeTimes + "\u0001"
                       + maxReconsumeTimes.ToString(), props);
        Assert.Contains(MessageConst.PropertyOriginMessageId + "\u0001MSG-ORIGIN-ID", props);
        // Half-message send-back must not look like a transaction check-back again.
        Assert.DoesNotContain(MessageConst.PropertyTransactionPrepared, props);
    }

    // Control: when the bounce is accepted there is no normal send at all.
    [Fact]
    public void AcceptedBounceSendsNothingElse()
    {
        using MockCluster cluster = MockCluster.Start(1);
        DefaultMQPushConsumer consumer = NewPushConsumer(cluster, "sendback-accepted");
        try
        {
            consumer.Start();
            MessageExt msg = Ext();
            Assert.True(consumer.SendMessageBack(msg, 3));

            Assert.Single(Bounces(cluster));
            Assert.Empty(NormalSends(cluster));
            // The source message is never mutated by a send-back.
            Assert.Equal("true", msg.GetProperty(MessageConst.PropertyTransactionPrepared));
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    [Fact]
    public void RefusedBounceFallsBackToANormalRetrySend()
    {
        using MockCluster cluster = MockCluster.Start(1);
        cluster.SetReplyCode(RequestCode.ConsumerSendMsgBack, ResponseCode.SystemError, "bounce rejected");
        DefaultMQPushConsumer consumer = NewPushConsumer(cluster, "sendback-fallback");
        try
        {
            consumer.Start();
            MessageExt msg = Ext();
            Assert.True(consumer.SendMessageBack(msg, 3));

            WireRecord bounce = Assert.Single(Bounces(cluster));
            Assert.Equal(PushGroup, bounce.Ext["group"]);
            Assert.Equal("3", bounce.Ext["delayLevel"]);
            // -1 (the untouched default) is reported to the broker as 16.
            Assert.Equal("16", bounce.Ext["maxReconsumeTimes"]);

            WireRecord send = Assert.Single(NormalSends(cluster));
            AssertRetryCopy(send, PushGroup, 16, 0, msg.Body);
            // The bounce really came first: the fallback is a reaction, not the normal path.
            List<WireRecord> records = cluster.Records();
            Assert.True(records.IndexOf(bounce) < records.IndexOf(send));
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    [Fact]
    public void BothLegsFailingRethrowsTheOriginalError()
    {
        using MockCluster cluster = MockCluster.Start(1);
        cluster.SetReplyCode(RequestCode.ConsumerSendMsgBack, ResponseCode.SystemError, "bounce rejected");
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.SystemError, 0));
        DefaultMQPushConsumer consumer = NewPushConsumer(cluster, "sendback-both-fail");
        try
        {
            consumer.Start();
            MQBrokerException e = Assert.Throws<MQBrokerException>(
                () => consumer.SendMessageBack(Ext(), 3));
            // The ORIGINAL failure, not the fallback's.
            Assert.Equal(ResponseCode.SystemError, e.ResponseCode);
            Assert.Equal("bounce rejected", e.ResponseMessage);
            Assert.Single(NormalSends(cluster));
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    [Fact]
    public void RetryCeilingKnobRidesOnTheBounceAndTheCopy()
    {
        using MockCluster cluster = MockCluster.Start(1);
        cluster.SetReplyCode(RequestCode.ConsumerSendMsgBack, ResponseCode.SystemError, "bounce rejected");
        DefaultMQPushConsumer consumer = NewPushConsumer(cluster, "sendback-knob");
        consumer.MaxReconsumeTimes = 5;
        try
        {
            consumer.Start();
            MessageExt msg = Ext(reconsumeTimes: 2);
            Assert.True(consumer.SendMessageBack(msg, 6));

            Assert.Equal("5", Assert.Single(Bounces(cluster)).Ext["maxReconsumeTimes"]);
            // 3 + reconsumeTimes, and the copy reports one attempt further along.
            WireRecord send = Assert.Single(NormalSends(cluster));
            AssertRetryCopy(send, PushGroup, 5, 2, msg.Body);
            Assert.Contains("DELAY\u00015", send.Ext["i"]);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    // ---------------- Pull consumer ----------------

    [Fact]
    public void PullRefusedBounceFallsBackAndCarriesTheDefaultCeiling()
    {
        using MockCluster cluster = MockCluster.Start(1);
        cluster.SetReplyCode(RequestCode.ConsumerSendMsgBack, ResponseCode.SystemError, "bounce rejected");
        DefaultMQPullConsumer consumer = NewPullConsumer(cluster, "sendback-pull-fallback");
        try
        {
            consumer.Start();
            // A pull consumer has no background rebalance, so the route has to be fetched
            // the way an application does it before the bounce can be addressed.
            Assert.NotEmpty(consumer.FetchSubscribeMessageQueues(Topic));
            MessageExt msg = Ext();
            consumer.SendMessageBack(msg, 2);

            WireRecord bounce = Assert.Single(Bounces(cluster));
            Assert.Equal(PullGroup, bounce.Ext["group"]);
            Assert.Equal("2", bounce.Ext["delayLevel"]);
            Assert.Equal("16", bounce.Ext["maxReconsumeTimes"]);

            WireRecord send = Assert.Single(NormalSends(cluster));
            AssertRetryCopy(send, PullGroup, 16, 0, msg.Body);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    [Fact]
    public void PullBothLegsFailingRethrowsTheOriginalError()
    {
        using MockCluster cluster = MockCluster.Start(1);
        cluster.SetReplyCode(RequestCode.ConsumerSendMsgBack, ResponseCode.SystemError, "bounce rejected");
        cluster.Script(0, new List<(int, int)>(), (ResponseCode.SystemError, 0));
        DefaultMQPullConsumer consumer = NewPullConsumer(cluster, "sendback-pull-both-fail");
        try
        {
            consumer.Start();
            Assert.NotEmpty(consumer.FetchSubscribeMessageQueues(Topic));
            MQBrokerException e = Assert.Throws<MQBrokerException>(
                () => consumer.SendMessageBack(Ext(), 2));
            Assert.Equal(ResponseCode.SystemError, e.ResponseCode);
            Assert.Equal("bounce rejected", e.ResponseMessage);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    [Fact]
    public void PullRetryCeilingDefaultsToSixteenAndIgnoresTheUnlimitedSentinel()
    {
        using MockCluster cluster = MockCluster.Start(1);
        cluster.SetReplyCode(RequestCode.ConsumerSendMsgBack, ResponseCode.SystemError, "bounce rejected");
        DefaultMQPullConsumer consumer = NewPullConsumer(cluster, "sendback-pull-knob");
        try
        {
            consumer.Start();
            Assert.NotEmpty(consumer.FetchSubscribeMessageQueues(Topic));
            MessageExt msg = Ext();
            consumer.SendMessageBack(msg, 2);
            Assert.Equal("16", Assert.Single(Bounces(cluster)).Ext["maxReconsumeTimes"]);

            consumer.MaxReconsumeTimes = 5;
            cluster.ClearRequests();
            consumer.SendMessageBack(msg, 2);
            Assert.Equal("5", Assert.Single(Bounces(cluster)).Ext["maxReconsumeTimes"]);
            AssertRetryCopy(Assert.Single(NormalSends(cluster)), PullGroup, 5, 0, msg.Body);

            consumer.MaxReconsumeTimes = -1;
            cluster.ClearRequests();
            consumer.SendMessageBack(msg, 2);
            Assert.Equal("16", Assert.Single(Bounces(cluster)).Ext["maxReconsumeTimes"]);
            AssertRetryCopy(Assert.Single(NormalSends(cluster)), PullGroup, 16, 0, msg.Body);
        }
        finally
        {
            consumer.Shutdown();
        }
    }

    [Fact]
    public void PullMissingMasterStillFallsBack()
    {
        using MockCluster cluster = MockCluster.Start(1);
        DefaultMQPullConsumer consumer = NewPullConsumer(cluster, "sendback-pull-no-master");
        try
        {
            consumer.Start();
            Assert.NotEmpty(consumer.FetchSubscribeMessageQueues(Topic));
            // No such broker in the route: the bounce cannot even be addressed, which is
            // exactly the case the fallback exists for.
            MessageExt msg = Ext(broker: "broker-absent");
            consumer.SendMessageBack(msg, 2);

            Assert.Empty(Bounces(cluster));
            AssertRetryCopy(Assert.Single(NormalSends(cluster)), PullGroup, 16, 0, msg.Body);
        }
        finally
        {
            consumer.Shutdown();
        }
    }
}
