
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class OrderlyReconsumeTests
{
    private const string Group = "GID_OrderlyReconsumeNetUnit";
    private const string Topic = "OrderlyReconsumeNetUnitTopic";
    private const string Broker = "broker-a";

    private static MessageQueue Queue0() => new(Topic, Broker, 0);

    private static MessageExt Ext(long queueOffset, int reconsumeTimes = 0) => new()
    {
        Topic = Topic,
        BrokerName = Broker,
        QueueId = 0,
        QueueOffset = queueOffset,
        ReconsumeTimes = reconsumeTimes,
        MsgId = "mid-" + queueOffset.ToString(CultureInfo.InvariantCulture),
        Body = Encoding.UTF8.GetBytes("x"),
    };

    private static List<MessageExt> OffsetBatch(long start, int[] reconsumeTimes)
        => reconsumeTimes
            .Select((rt, i) => Ext(start + i, rt))
            .ToList();

    /// <summary>An orderly listener returning a fixed state (the orderly side has no
    /// ackIndex, only a state).</summary>
    private sealed class OrderlyListener : IMessageListenerOrderly
    {
        private readonly ConsumeOrderlyStatus _status;
        private bool? _autoCommit;
        private int? _suspendMs;

        public OrderlyListener(ConsumeOrderlyStatus status) => _status = status;

        /// <summary>Simulates a binlog consumer: the listener sets AutoCommit=false and
        /// takes the commit right.</summary>
        public OrderlyListener ManualCommit(bool autoCommit)
        {
            _autoCommit = autoCommit;
            return this;
        }

        /// <summary>Specifies the suspend duration on the context (-1 = not specified,
        /// fall back to the consumer config).</summary>
        public OrderlyListener Asking(int ms)
        {
            _suspendMs = ms;
            return this;
        }

        public bool Orderly() => true;

        public ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs,
                                                   ConsumeOrderlyContext context)
        {
            if (_autoCommit.HasValue)
            {
                context.AutoCommit = _autoCommit.Value;
            }

            if (_suspendMs.HasValue)
            {
                context.SuspendCurrentQueueTimeMillis = _suspendMs.Value;
            }

            return _status;
        }
    }

    /// <summary>
    /// An orderly consumer that is "not Started": a resend is bound to fail
    /// (<c>Client()</c> throws -> swallowed internally -> false returned), which is
    /// exactly the fixture for the "a failed resend must not advance the offset" branch.
    /// </summary>
    private sealed class Harness
    {
        private readonly DefaultMQPushConsumer _consumer;
        private readonly string _key;

        public Harness(int maxReconsumeTimes, List<MessageExt>? pending = null)
        {
            _consumer = new DefaultMQPushConsumer(Group)
            {
                MaxReconsumeTimes = maxReconsumeTimes,
            };
            _consumer.SetMessageListener(new OrderlyListener(
                ConsumeOrderlyStatus.SuspendCurrentQueueAMoment));
            _key = DefaultMQPushConsumer.OffsetKeyForTest(Queue0());
            // On a real machine dispatchLoop is "take this batch out of the buffer first,
            // then consume", so the key is still in the table (it may even have some
            // remaining allowance). What a failed resend has to put back is exactly this
            // table; not seeding it would mean the queue has already been withdrawn.
            _consumer.SetPendingForTest(_key, pending ?? new List<MessageExt>());
        }

        public bool Run(List<MessageExt> batch)
            => _consumer.ConsumeBatchForTest(_key, Queue0(), batch);

        /// <summary>Runs a batch with the given listener (optionally with AutoCommit /
        /// suspend duration settings).</summary>
        public bool RunWith(IMessageListenerOrderly listener, List<MessageExt> batch,
                            List<MessageExt>? pending = null)
        {
            if (pending is not null)
            {
                _consumer.SetPendingForTest(_key, pending);
            }

            _consumer.SetMessageListener(listener);
            return _consumer.ConsumeBatchForTest(_key, Queue0(), batch);
        }

        public DefaultMQPushConsumer Consumer() => _consumer;

        public List<(long Offset, int Times)> Pending()
            => _consumer.PendingForTest(_key).Select(m => (m.QueueOffset, m.ReconsumeTimes)).ToList();

        public long? Offset() => _consumer.ConsumeOffsetForTest(_key);

        public bool CheckGate(List<MessageExt> batch)
            => _consumer.CheckOrderlyReconsumeTimes(batch);

        public int OrderlyMax() => _consumer.OrderlyMaxReconsumeTimes();

        public int ConcurrentMax() => _consumer.MaxReconsumeTimesOrDefault();

        public Message BuildRetry(MessageExt poison, int maxTimes)
            => _consumer.BuildRetryMessageForTest(poison, maxTimes);
    }

    [Fact]
    public void MinusOneMeansUnlimitedOnTheOrderlySideNotTheConcurrentSixteen()
    {
        var h = new Harness(-1);
        Assert.Equal(int.MaxValue, h.OrderlyMax());
        Assert.Equal(16, h.ConcurrentMax());

        var set = new Harness(3);
        Assert.Equal(3, set.OrderlyMax());
        Assert.Equal(3, set.ConcurrentMax());
    }

    [Fact]
    public void DefaultCapNeverJudgesThePoisonMessageExhausted()
    {
        // Under the default config, even once reconsumeTimes has reached int.MaxValue-1 it
        // is only +1 locally and keeps retrying by suspending; it does not resend (and
        // certainly is not put into %DLQ% by the broker).
        var h = new Harness(-1);
        List<MessageExt> batch = OffsetBatch(0, new[] { int.MaxValue - 1 });
        Assert.True(h.CheckGate(batch));
        Assert.Equal(int.MaxValue, batch[0].ReconsumeTimes);
    }

    [Fact]
    public void BelowCapCountsLocallyAndSuspends()
    {
        // It does not resend at all when not exhausted (and it cannot resend while not
        // Started either, so this criterion has to go through the criterion function itself)
        var h = new Harness(3);
        List<MessageExt> batch = OffsetBatch(0, new[] { 0, 2 });
        Assert.True(h.CheckGate(batch));
        Assert.Equal(new[] { 1, 3 }, batch.Select(m => m.ReconsumeTimes).ToArray());
    }

    [Fact]
    public void AtCapWithFailedSendBackStillSuspends()
    {
        var h = new Harness(2);
        List<MessageExt> batch = OffsetBatch(7, new[] { 2 });
        Assert.True(h.CheckGate(batch));
        Assert.Equal(3, batch[0].ReconsumeTimes);
    }

    [Fact]
    public void EmptyBatchDoesNotSuspend()
    {
        var h = new Harness(0);
        Assert.False(h.CheckGate(new List<MessageExt>()));
        Assert.False(h.CheckGate(null!));
    }

    [Fact]
    public void RetryMessageCarriesExactlyLegacysFields()
    {
        var h = new Harness(2);
        MessageExt poison = OffsetBatch(7, new[] { 2 })[0];
        poison.PutProperty(MessageConst.PropertyTransactionPrepared, "true");
        poison.PutProperty(MessageConst.PropertyKeys, "k7");
        Message newMsg = h.BuildRetry(poison, h.OrderlyMax());

        Assert.Equal("%RETRY%GID_OrderlyReconsumeNetUnit", newMsg.Topic);
        Assert.Equal(poison.Body, newMsg.Body);
        Assert.Equal("k7", newMsg.GetProperty(MessageConst.PropertyKeys));
        Assert.Equal(Topic, newMsg.GetProperty(MessageConst.PropertyRetryTopic));
        Assert.Equal("3", newMsg.GetProperty(MessageConst.PropertyReconsumeTime));
        Assert.Equal("2", newMsg.GetProperty(MessageConst.PropertyMaxReconsumeTimes));
        Assert.Equal(5, newMsg.DelayTimeLevel);
        Assert.Equal("mid-7", newMsg.GetProperty(MessageConst.PropertyOriginMessageId));
        Assert.False(newMsg.Properties.ContainsKey(MessageConst.PropertyTransactionPrepared),
            "the half-message marker must be cleared, otherwise the broker treats it as a check message");
        Assert.False(string.IsNullOrEmpty(newMsg.GetProperty(MessageConst.PropertyUniqClientMessageIdKeyidx)),
            "a resend must carry UNIQ_KEY too, otherwise the trace cannot be tied together");
    }

    [Fact]
    public void SuspendRequeuesBatchWhileBelowCap()
    {
        // There is a next one at the tail (offset 2), this batch must be inserted in front of it in the original order
        var h = new Harness(3, OffsetBatch(2, new[] { 0 }));
        Assert.False(h.Run(OffsetBatch(0, new[] { 0, 1 })));
        Assert.Null(h.Offset());
        Assert.Equal(new[] { (0L, 1), (1L, 2), (2L, 0) }, h.Pending().ToArray());
    }

    [Fact]
    public void SuccessPathSkipsTheReconsumeGate()
    {
        // Threshold 0: as soon as the criterion is reached it attempts a resend (which
        // offline always fails -> suspend), so "the offset advanced" itself proves SUCCESS
        // never touched the criterion.
        var c = new DefaultMQPushConsumer(Group) { MaxReconsumeTimes = 0 };
        c.SetMessageListener(new OrderlyListener(ConsumeOrderlyStatus.Success));
        string key = DefaultMQPushConsumer.OffsetKeyForTest(Queue0());
        c.SetPendingForTest(key, new List<MessageExt>());
        Assert.True(c.ConsumeBatchForTest(key, Queue0(), OffsetBatch(0, new[] { 0 })));
        Assert.Equal(1, c.ConsumeOffsetForTest(key));
        Assert.Empty(c.PendingForTest(key));
    }

    // ---------------- Suspend duration (context first + clamping) ----------------

    [Fact]
    public void OrderlySuspendMillisResolvesThenClampsLikeLegacy()
    {
        var h = new Harness(0);
        h.Consumer().SuspendCurrentQueueTimeMillis = 900;
        var ctx = new ConsumeOrderlyContext();
        int Resolve(int asked)
        {
            ctx.SuspendCurrentQueueTimeMillis = asked;
            return h.Consumer().OrderlySuspendMillis(ctx);
        }

        Assert.Equal(900, Resolve(-1));       // not specified -> fall back to the config
        Assert.Equal(70, Resolve(70));        // the context wins
        Assert.Equal(10, Resolve(0));         // clamped at the lower bound
        Assert.Equal(10, Resolve(9));
        Assert.Equal(10, Resolve(10));
        Assert.Equal(30000, Resolve(30000));
        Assert.Equal(30000, Resolve(30001));  // clamped at the upper bound
        Assert.Equal(30000, Resolve(int.MaxValue));

        h.Consumer().SuspendCurrentQueueTimeMillis = -1;
        Assert.Equal(10, Resolve(-1));
    }

    // ---------------- Illegal states under AutoCommit=true ----------------

    [Theory]
    [InlineData(ConsumeOrderlyStatus.Commit)]
    [InlineData(ConsumeOrderlyStatus.Rollback)]
    public void IllegalCommitAndRollbackUnderAutoCommitAreAcked(ConsumeOrderlyStatus status)
    {
        var h = new Harness(0, OffsetBatch(2, new[] { 0 }));
        Assert.True(h.RunWith(new OrderlyListener(status), OffsetBatch(0, new[] { 0, 0 })));
        Assert.Equal(2, h.Offset());
        Assert.Single(h.Pending());  // the tail one is still there, this batch was not resent
    }

    // ---------------- AutoCommit=false (manual commit) ----------------

    [Fact]
    public void ManualCommitAdvancesWithoutRequeueing()
    {
        var h = new Harness(0);
        var listener = new OrderlyListener(ConsumeOrderlyStatus.Commit).ManualCommit(false);
        Assert.True(h.RunWith(listener, OffsetBatch(0, new[] { 0, 0 })));
        Assert.Equal(2, h.Offset());
        Assert.Empty(h.Pending());
    }

    [Fact]
    public void ManualRollbackRequeuesAndHoldsTheOffset()
    {
        var h = new Harness(0);
        var listener = new OrderlyListener(ConsumeOrderlyStatus.Rollback).ManualCommit(false);
        Assert.False(h.RunWith(listener, OffsetBatch(0, new[] { 0, 0 }),
            pending: OffsetBatch(2, new[] { 0 })));
        Assert.Null(h.Offset());
        Assert.Equal(new[] { (0L, 0), (1L, 0), (2L, 0) }, h.Pending().ToArray());
    }

    [Fact]
    public void ManualSuccessLeavesTheBatchWithoutCommitting()
    {
        var h = new Harness(0);
        var listener = new OrderlyListener(ConsumeOrderlyStatus.Success).ManualCommit(false);
        Assert.True(h.RunWith(listener, OffsetBatch(0, new[] { 0 })));
        Assert.Null(h.Offset());
        Assert.Empty(h.Pending());
    }

    [Fact]
    public void ManualSuspendNeverCommitsTheOffset()
    {
        var h = new Harness(0);
        var listener = new OrderlyListener(ConsumeOrderlyStatus.SuspendCurrentQueueAMoment)
            .ManualCommit(false);
        Assert.False(h.RunWith(listener, OffsetBatch(0, new[] { 0 })));
        Assert.Null(h.Offset());
        Assert.Equal(new[] { (0L, 1) }, h.Pending().ToArray());  // put back after the local +1
    }

    // ---------------- The mapping of 309 CONSUME_MESSAGE_DIRECTLY ----------------

    [Theory]
    [InlineData(ConsumeOrderlyStatus.Success, "CR_SUCCESS")]
    [InlineData(ConsumeOrderlyStatus.SuspendCurrentQueueAMoment, "CR_LATER")]
    [InlineData(ConsumeOrderlyStatus.Commit, "CR_COMMIT")]
    [InlineData(ConsumeOrderlyStatus.Rollback, "CR_ROLLBACK")]
    public void DirectConsumeMapsOrderlyStatuses(ConsumeOrderlyStatus status, string expect)
    {
        var c = new DefaultMQPushConsumer(Group);
        c.SetMessageListener(new OrderlyListener(status).ManualCommit(false));
        ConsumeMessageDirectlyResult r = c.ConsumeMessageDirectly(Ext(0), Broker);
        Assert.Equal(expect, r.ConsumeResult);
        Assert.True(r.Order);
        Assert.False(r.AutoCommit);  // the false the listener set must go back to the broker as is
    }

    [Fact]
    public void DirectConsumeWithoutListenerIsReturnNull()
    {
        var c = new DefaultMQPushConsumer(Group);
        ConsumeMessageDirectlyResult r = c.ConsumeMessageDirectly(Ext(0), Broker);
        Assert.Equal("CR_RETURN_NULL", r.ConsumeResult);
    }

    // ---------------- %RETRY% properties lifted into the send header ----------------

    [Fact]
    public void RetryTopicPropertiesAreLiftedIntoTheSendHeader()
    {
        var inst = NewInstance();
        var msg = new Message("%RETRY%" + Group, Encoding.UTF8.GetBytes("poison"));
        msg.PutProperty(MessageConst.PropertyReconsumeTime, "4");
        msg.PutProperty(MessageConst.PropertyMaxReconsumeTimes, "6");
        RemotingCommand request = inst.BuildSendRequest("PG_Lift", msg,
            new MessageQueue("%RETRY%" + Group, Broker, 0));
        request.MakeCustomHeaderToNet();

        Assert.Equal("4", request.ExtFields["j"]);
        Assert.Equal("6", request.ExtFields["l"]);
        // Note: RECONSUME_TIME is still kept in the on-wire properties: the consumer side
        // relies on it to reconstruct the retry count
        Assert.Contains(MessageConst.PropertyReconsumeTime, request.ExtFields["i"]);
    }

    [Fact]
    public void OrdinaryTopicNeverLiftsReconsumeTimes()
    {
        var inst = NewInstance();
        var msg = new Message(Topic, Encoding.UTF8.GetBytes("normal"));
        msg.PutProperty(MessageConst.PropertyReconsumeTime, "4");
        msg.PutProperty(MessageConst.PropertyMaxReconsumeTimes, "6");
        RemotingCommand request = inst.BuildSendRequest("PG_Normal", msg, Queue0());
        request.MakeCustomHeaderToNet();

        Assert.Equal("0", request.ExtFields["j"]);
        Assert.False(request.ExtFields.ContainsKey("l"), "not configured must not go on the wire");
    }

    private static MQClientInstance NewInstance() => new(
        "OrderlyLift_" + Environment.CurrentManagedThreadId.ToString(CultureInfo.InvariantCulture),
        new List<string> { "127.0.0.1:9876" });
}
