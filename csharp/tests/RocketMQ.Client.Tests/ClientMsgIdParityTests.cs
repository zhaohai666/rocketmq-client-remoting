// Java MessageClientExt#getMsgId() parity on consumed messages.
//
// The client-side decoder puts UNIQ_KEY into MsgId and keeps the broker offset id in
// OffsetMsgId; MessageExt.ClientMsgId is the single helper (UNIQ_KEY wins, offset id is
// the fallback) that every consume-side call site must use: the SubBefore/SubAfter bean,
// CONSUMER_SEND_MSG_BACK.originMsgId, and the retry ORIGIN_MESSAGE_ID fallback.
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class ClientMsgIdParityTests
{
    private const string OffsetId = "0A0B0C0D00002A9F0000000000000EA7";
    private const string UniqId = "AC1E0A0B2C3D4E5F60718293A4B5C6D7";
    private const string Topic = "ClientMsgIdParityTopic";
    private const string Broker = "broker-0";
    private const string PushGroup = "GID_client_msg_id_parity_push";
    private const string PullGroup = "GID_client_msg_id_parity_pull";

    // A consumed message shaped like the decoder's output: OffsetMsgId is the broker
    // offset id and MsgId is deliberately left as that same offset id. A helper that just
    // read MsgId would NOT return UNIQ_KEY, so the property is the discriminator.
    private static MessageExt Consumed(bool withUniq)
    {
        var msg = new MessageExt
        {
            Topic = Topic,
            BrokerName = Broker,
            QueueId = 0,
            QueueOffset = 7,
            CommitLogOffset = 4242,
            MsgId = OffsetId,
            OffsetMsgId = OffsetId,
            Body = Encoding.UTF8.GetBytes("client-msg-id-body"),
            ReconsumeTimes = 0,
        };
        if (withUniq)
        {
            msg.PutProperty(MessageConst.PropertyUniqClientMessageIdKeyidx, UniqId);
        }

        return msg;
    }

    private sealed class NoopListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context) => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }

    private static ConsumeMessageContext Context(MessageExt msg) => new("CID_test",
        new List<MessageExt> { msg }, new MessageQueue(Topic, Broker, 0))
    {
        Props = new SortedDictionary<string, string>(),
        AccessChannel = RocketMQ.Client.AccessChannel.Local,
        Success = true,
    };

    private static List<TraceContext> DrainQueue(AsyncTraceDispatcher d)
    {
        FieldInfo field = typeof(AsyncTraceDispatcher).GetField("_queue",
            BindingFlags.NonPublic | BindingFlags.Instance)!;
        var queue = (BlockingCollection<TraceContext>)field.GetValue(d)!;
        var list = new List<TraceContext>();
        while (queue.TryTake(out TraceContext? ctx, TimeSpan.Zero) && ctx is not null)
        {
            list.Add(ctx);
        }

        return list;
    }

    // ---------------------------------------------------------------- 1. the helper
    [Fact]
    public void ClientMsgIdPrefersUniqKeyAndFallsBackToOffsetId()
    {
        Assert.Equal(UniqId, Consumed(true).ClientMsgId);
        Assert.Equal(OffsetId, Consumed(false).ClientMsgId);
    }

    // ---------------------------------------------------------------- 2. consume-side SubBefore / SubAfter
    [Fact]
    public void ConsumeSubBeforeAndAfterCarryTheUniqKey()
    {
        var d = new AsyncTraceDispatcher("CID_test", TraceDispatcherType.Consume, 10, null, null);
        var hook = new ConsumeMessageTraceHook(d);
        ConsumeMessageContext ctx = Context(Consumed(true));
        ctx.Props!["ConsumeContextType"] = "SUCCESS";

        hook.ConsumeMessageBefore(ctx);
        var before = Assert.IsType<TraceContext>(ctx.MqTraceContext);
        Assert.Equal(RocketMQ.Client.TraceType.SubBefore, before.TraceType);
        Assert.Equal(UniqId, before.TraceBeans[0].MsgId);
        Assert.NotEqual(OffsetId, before.TraceBeans[0].MsgId);

        hook.ConsumeMessageAfter(ctx);
        List<TraceContext> queued = DrainQueue(d);
        Assert.Equal(2, queued.Count);
        Assert.Equal(RocketMQ.Client.TraceType.SubAfter, queued[1].TraceType);
        // SubAfter reuses the SubBefore beans, so its msgId is the UNIQ_KEY too.
        Assert.Equal(UniqId, queued[1].TraceBeans[0].MsgId);
        Assert.Equal(queued[0].RequestId, queued[1].RequestId);
    }

    [Fact]
    public void ConsumeSubBeforeFallsBackToOffsetIdWithoutUniqKey()
    {
        var d = new AsyncTraceDispatcher("CID_test", TraceDispatcherType.Consume, 10, null, null);
        var hook = new ConsumeMessageTraceHook(d);
        ConsumeMessageContext ctx = Context(Consumed(false));

        hook.ConsumeMessageBefore(ctx);
        var before = Assert.IsType<TraceContext>(ctx.MqTraceContext);
        Assert.Equal(OffsetId, before.TraceBeans[0].MsgId);
    }

    // ---------------------------------------------------------------- 3. retry ORIGIN_MESSAGE_ID
    [Fact]
    public void RetryOriginMessageIdIsTheUniqKey()
    {
        var c = new DefaultMQPushConsumer(PushGroup);
        Message withUniq = c.BuildRetryMessageForTest(Consumed(true), 16);
        Assert.Equal(UniqId, withUniq.GetProperty(MessageConst.PropertyOriginMessageId));
        Assert.NotEqual(OffsetId, withUniq.GetProperty(MessageConst.PropertyOriginMessageId));

        Message withoutUniq = c.BuildRetryMessageForTest(Consumed(false), 16);
        Assert.Equal(OffsetId, withoutUniq.GetProperty(MessageConst.PropertyOriginMessageId));
    }

    // ---------------------------------------------------------------- 4. send-back header + retry copy (wire, push + pull)
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public void SendBackHeaderAndRetryCopyCarryTheUniqKey(bool push)
    {
        using MockCluster cluster = MockCluster.Start(1);
        // A refused bounce degrades to a normal %RETRY% send, so both the
        // CONSUMER_SEND_MSG_BACK header and the retry copy can be read off the wire.
        cluster.SetReplyCode(RequestCode.ConsumerSendMsgBack, ResponseCode.SystemError,
            "bounce rejected");
        MessageExt msg = Consumed(true);

        if (push)
        {
            var consumer = new DefaultMQPushConsumer(PushGroup) { InstanceName = "client-msg-id-push" };
            consumer.SetNamesrvAddr(cluster.NamesrvAddr);
            consumer.Subscribe(Topic, "*");
            consumer.SetMessageListener(new NoopListener());
            try
            {
                consumer.Start();
                consumer.SendMessageBack(msg, 3);
                AssertUniqOnWire(cluster);
            }
            finally
            {
                consumer.Shutdown();
            }
        }
        else
        {
            var consumer = new DefaultMQPullConsumer(PullGroup);
            consumer.SetInstanceName("client-msg-id-pull");
            consumer.SetNamesrvAddr(cluster.NamesrvAddr);
            try
            {
                consumer.Start();
                Assert.NotEmpty(consumer.FetchSubscribeMessageQueues(Topic));
                consumer.SendMessageBack(msg, 2);
                AssertUniqOnWire(cluster);
            }
            finally
            {
                consumer.Shutdown();
            }
        }
    }

    private static void AssertUniqOnWire(MockCluster cluster)
    {
        WireRecord bounce = Assert.Single(cluster.Records(),
            r => r.Code == RequestCode.ConsumerSendMsgBack);
        Assert.Equal(UniqId, bounce.Ext["originMsgId"]);
        Assert.NotEqual(OffsetId, bounce.Ext["originMsgId"]);

        WireRecord send = Assert.Single(cluster.Records(),
            r => r.Code == RequestCode.SendMessageV2);
        string props = send.Ext["i"];
        Assert.Contains(MessageConst.PropertyOriginMessageId + "\u0001" + UniqId, props);
        Assert.DoesNotContain(MessageConst.PropertyOriginMessageId + "\u0001" + OffsetId, props);
    }
}
