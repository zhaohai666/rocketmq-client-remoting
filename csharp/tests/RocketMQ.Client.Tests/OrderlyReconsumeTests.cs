
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

    /// <summary>返回固定状态的顺序 listener（顺序侧没有 ackIndex，只有状态）。</summary>
    private sealed class OrderlyListener : IMessageListenerOrderly
    {
        private readonly ConsumeOrderlyStatus _status;
        private bool? _autoCommit;
        private int? _suspendMs;

        public OrderlyListener(ConsumeOrderlyStatus status) => _status = status;

        /// <summary>模拟 binlog 消费方：listener 置 AutoCommit=false 拿走提交权。</summary>
        public OrderlyListener ManualCommit(bool autoCommit)
        {
            _autoCommit = autoCommit;
            return this;
        }

        /// <summary>在 context 上指定挂起时长（-1 = 没指定，回落到消费者配置）。</summary>
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
    /// 一把「未 Start」的顺序消费者：回投必定失败（<c>Client()</c> 抛 → 内部吞掉 → 返回
    /// false），正是「回投失败不能推进位点」这条分支的夹具。
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
            // 真机里 dispatchLoop 是「先从缓冲取走本批、再消费」，所以 key 一定还在表里
            //（可能还有余量）。回投失败要塞回的正是这张表；不预置就等于队列已被撤走。
            _consumer.SetPendingForTest(_key, pending ?? new List<MessageExt>());
        }

        public bool Run(List<MessageExt> batch)
            => _consumer.ConsumeBatchForTest(_key, Queue0(), batch);

        /// <summary>用指定 listener（可带 AutoCommit / 挂起时长设定）跑一批。</summary>
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
        // 默认配置下即便 reconsumeTimes 已到 int.MaxValue-1，也只是本地 +1 继续挂起重试，
        // 不会去回投（更不会被 broker 投进 %DLQ%）。
        var h = new Harness(-1);
        List<MessageExt> batch = OffsetBatch(0, new[] { int.MaxValue - 1 });
        Assert.True(h.CheckGate(batch));
        Assert.Equal(int.MaxValue, batch[0].ReconsumeTimes);
    }

    [Fact]
    public void BelowCapCountsLocallyAndSuspends()
    {
        // 没用尽时压根不回投（未 Start 也回投不了，所以这条判据必须自己走判据函数）
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
            "半消息标记必须清掉，否则 broker 会把它当回查消息");
        Assert.False(string.IsNullOrEmpty(newMsg.GetProperty(MessageConst.PropertyUniqClientMessageIdKeyidx)),
            "回投也要带 UNIQ_KEY，否则轨迹串不起来");
    }

    [Fact]
    public void SuspendRequeuesBatchWhileBelowCap()
    {
        // 队尾还有下一条（offset 2），本批要按原顺序插到它前面
        var h = new Harness(3, OffsetBatch(2, new[] { 0 }));
        Assert.False(h.Run(OffsetBatch(0, new[] { 0, 1 })));
        Assert.Null(h.Offset());
        Assert.Equal(new[] { (0L, 1), (1L, 2), (2L, 0) }, h.Pending().ToArray());
    }

    [Fact]
    public void SuccessPathSkipsTheReconsumeGate()
    {
        // 阈值 0：一旦走到判据就会尝试回投（离线必败 → 挂起），所以「位点前进」
        // 本身就证明了 SUCCESS 没碰判据。
        var c = new DefaultMQPushConsumer(Group) { MaxReconsumeTimes = 0 };
        c.SetMessageListener(new OrderlyListener(ConsumeOrderlyStatus.Success));
        string key = DefaultMQPushConsumer.OffsetKeyForTest(Queue0());
        c.SetPendingForTest(key, new List<MessageExt>());
        Assert.True(c.ConsumeBatchForTest(key, Queue0(), OffsetBatch(0, new[] { 0 })));
        Assert.Equal(1, c.ConsumeOffsetForTest(key));
        Assert.Empty(c.PendingForTest(key));
    }

    // ---------------- 挂起时长（context 优先 + 钳位） ----------------

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

        Assert.Equal(900, Resolve(-1));       // 没指定 → 回落配置
        Assert.Equal(70, Resolve(70));        // context 优先
        Assert.Equal(10, Resolve(0));         // 钳下限
        Assert.Equal(10, Resolve(9));
        Assert.Equal(10, Resolve(10));
        Assert.Equal(30000, Resolve(30000));
        Assert.Equal(30000, Resolve(30001));  // 钳上限
        Assert.Equal(30000, Resolve(int.MaxValue));

        h.Consumer().SuspendCurrentQueueTimeMillis = -1;
        Assert.Equal(10, Resolve(-1));
    }

    // ---------------- AutoCommit=true 下的非法状态 ----------------

    [Theory]
    [InlineData(ConsumeOrderlyStatus.Commit)]
    [InlineData(ConsumeOrderlyStatus.Rollback)]
    public void IllegalCommitAndRollbackUnderAutoCommitAreAcked(ConsumeOrderlyStatus status)
    {
        var h = new Harness(0, OffsetBatch(2, new[] { 0 }));
        Assert.True(h.RunWith(new OrderlyListener(status), OffsetBatch(0, new[] { 0, 0 })));
        Assert.Equal(2, h.Offset());
        Assert.Single(h.Pending());  // 队尾那条还在，本批没回投
    }

    // ---------------- AutoCommit=false（手动提交） ----------------

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
    public void ManualSuccessHoldsTheBatchWithoutCommitting()
    {
        var h = new Harness(0);
        var listener = new OrderlyListener(ConsumeOrderlyStatus.Success).ManualCommit(false);
        Assert.False(h.RunWith(listener, OffsetBatch(0, new[] { 0 })));
        Assert.Null(h.Offset());
        Assert.Equal(new[] { (0L, 0) }, h.Pending().ToArray());
    }

    [Fact]
    public void ManualSuspendNeverCommitsTheOffset()
    {
        var h = new Harness(0);
        var listener = new OrderlyListener(ConsumeOrderlyStatus.SuspendCurrentQueueAMoment)
            .ManualCommit(false);
        Assert.False(h.RunWith(listener, OffsetBatch(0, new[] { 0 })));
        Assert.Null(h.Offset());
        Assert.Equal(new[] { (0L, 1) }, h.Pending().ToArray());  // 本地 +1 后塞回
    }

    // ---------------- 309 CONSUME_MESSAGE_DIRECTLY 的映射 ----------------

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
        Assert.False(r.AutoCommit);  // listener 设的 false 要原样回给 broker
    }

    [Fact]
    public void DirectConsumeWithoutListenerIsReturnNull()
    {
        var c = new DefaultMQPushConsumer(Group);
        ConsumeMessageDirectlyResult r = c.ConsumeMessageDirectly(Ext(0), Broker);
        Assert.Equal("CR_RETURN_NULL", r.ConsumeResult);
    }

    // ---------------- %RETRY% 属性抬进发送头 ----------------

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
        // ⚠ 线上属性里 RECONSUME_TIME 仍然保留：消费端要靠它还原重试次数
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
        Assert.False(request.ExtFields.ContainsKey("l"), "没配就不该上线");
    }

    private static MQClientInstance NewInstance() => new(
        "OrderlyLift_" + Environment.CurrentManagedThreadId.ToString(CultureInfo.InvariantCulture),
        new List<string> { "127.0.0.1:9876" });
}
