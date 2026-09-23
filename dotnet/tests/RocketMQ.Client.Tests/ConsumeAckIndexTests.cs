// classic 并发消费路径的 ackIndex 语义单测 —— 不需要集群。
//
// 为什么必须离线锁死：这条路径错了是**静默丢消息**。Java
// ConsumeMessageConcurrentlyService#processConsumeResult:207-269 用 listener 写的
// ackIndex 把本批切成「已认可前缀 / 待回投后缀」，RECONSUME_LATER 强制 ackIndex=-1
//（整批回投）。两个方向写错在真机短期窗口里都看不出差别：
//   - 忽略 ackIndex：尾巴既没回投也没重投，直接丢；
//   - 默认值写成 -1：CONSUME_SUCCESS 也把整批回投，消息无限重复。
// 未 Start 的消费者 SendMessageBack 必定抛，所以这里能锁的是「失败分支」
//（回投失败 → 塞回队首 + 位点不越过它）；「回投成功 → 位点整批前进」只能靠真机
//（examples 的 ackIndex 场景）。与 Python/Rust/C++ 的同名测试一一对应。

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class ConsumeAckIndexTests
{
    private const string Group = "GID_AckIndexNetUnit";
    private const string Topic = "AckIndexNetUnitTopic";
    private const string Broker = "broker-a";

    private static MessageQueue Queue0() => new(Topic, Broker, 0);

    private static MessageExt Ext(long queueOffset, int reconsumeTimes = 0) => new()
    {
        Topic = Topic,
        BrokerName = Broker,
        QueueId = 0,
        QueueOffset = queueOffset,
        ReconsumeTimes = reconsumeTimes,
    };

    private static List<MessageExt> OffsetBatch(int n)
        => Enumerable.Range(0, n).Select(i => Ext(i)).ToList();

    /// <summary>按 Java listener 契约：可选地把 ackIndex 收窄，返回指定状态或直接抛异常。</summary>
    private sealed class AckListener : IMessageListenerConcurrently
    {
        private readonly ConsumeConcurrentlyStatus _status;
        private readonly int? _ackIndex;
        private readonly bool _throw;

        public AckListener(ConsumeConcurrentlyStatus status, int? ackIndex, bool @throw = false)
        {
            _status = status;
            _ackIndex = ackIndex;
            _throw = @throw;
        }

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
                                                       ConsumeConcurrentlyContext context)
        {
            if (_ackIndex.HasValue)
            {
                context.AckIndex = _ackIndex.Value;
            }

            if (_throw)
            {
                throw new InvalidOperationException("listener blew up");
            }

            return _status;
        }
    }

    /// <summary>
    /// 一把「未 Start」的消费者：SendMessageBack 必定失败，正是「回投失败不能推进位点」
    /// 这条分支的夹具。真机里 dispatchLoop 是「先从缓冲取走本批、再消费」，所以 key 一定
    /// 还在表里（可能还有余量）；回投失败要塞回的正是这张表。
    /// </summary>
    private sealed class Harness
    {
        private readonly DefaultMQPushConsumer _consumer;
        private readonly string _key;

        public Harness(string model = MessageModel.Clustering)
        {
            _consumer = new DefaultMQPushConsumer(Group) { MessageModel = model };
            _key = DefaultMQPushConsumer.OffsetKeyForTest(Queue0());
            _consumer.SetPendingForTest(_key, new List<MessageExt>());
        }

        public bool Run(List<MessageExt> batch, ConsumeConcurrentlyStatus status, int? ackIndex,
                        bool @throw = false)
        {
            _consumer.SetMessageListener(new AckListener(status, ackIndex, @throw));
            return _consumer.ConsumeBatchForTest(_key, Queue0(), batch);
        }

        /// <summary>队首缓冲的 (queueOffset, reconsumeTimes) 列表，便于一次性比对。</summary>
        public List<(long Offset, int Times)> Pending()
            => _consumer.PendingForTest(_key).Select(m => (m.QueueOffset, m.ReconsumeTimes)).ToList();

        public long? Offset() => _consumer.ConsumeOffsetForTest(_key);

        public long Consumed() => _consumer.ConsumedCount;
    }

    [Fact]
    public void ContextDefaultsLikeJava()
    {
        // Java ConsumeConcurrentlyContext.ackIndex = Integer.MAX_VALUE（= 整批认可）。
        // 默认值写成 -1 会让每次 CONSUME_SUCCESS 都整批回投 —— 消息无限重复。
        var ctx = new ConsumeConcurrentlyContext(Queue0());
        Assert.Equal(int.MaxValue, ctx.AckIndex);
        Assert.Equal(0, ctx.DelayLevelWhenNextConsume);
    }

    [Fact]
    public void DefaultAckIndexSendsNothingBack()
    {
        var h = new Harness();
        Assert.True(h.Run(OffsetBatch(3), ConsumeConcurrentlyStatus.ConsumeSuccess, null));
        Assert.Empty(h.Pending());
        Assert.Equal(3, h.Offset());
        Assert.Equal(3, h.Consumed());
    }

    [Fact]
    public void AckIndexWiderThanBatchIsClamped()
    {
        // listener 写了 99（Java:207-211 钳到 size-1）：等价整批认可
        var h = new Harness();
        h.Run(OffsetBatch(3), ConsumeConcurrentlyStatus.ConsumeSuccess, 99);
        Assert.Empty(h.Pending());
        Assert.Equal(3, h.Offset());
    }

    [Fact]
    public void PartialAckBacksTheTailAndHoldsTheOffsetAtTheFirstUnacked()
    {
        // 部分 ack（认可第 0 条）：尾巴 [1,2] 回投；离线回投必败 → 塞回队首
        //（reconsumeTimes +1），位点钳在第 1 条不越过它（Java removeMessage 返回 firstKey），
        // 已认可的第 0 条计入消费量
        var h = new Harness();
        Assert.False(h.Run(OffsetBatch(3), ConsumeConcurrentlyStatus.ConsumeSuccess, 0));
        Assert.Equal(new List<(long, int)> { (1, 1), (2, 1) }, h.Pending());
        Assert.Equal(1, h.Offset());
        Assert.Equal(1, h.Consumed());
    }

    [Fact]
    public void NegativeAckIndexBacksEveryMessageAndFreezesTheOffset()
    {
        // ackIndex=-1（一条都不认可）：整批回投，位点原地不动（没有任何条目被处理完）
        var h = new Harness();
        h.Run(OffsetBatch(3), ConsumeConcurrentlyStatus.ConsumeSuccess, -1);
        Assert.Equal(new List<(long, int)> { (0, 1), (1, 1), (2, 1) }, h.Pending());
        Assert.Null(h.Offset());
        Assert.Equal(0, h.Consumed());
    }

    [Fact]
    public void ReconsumeLaterOverridesAWiderAckIndex()
    {
        // Java:210-212 —— RECONSUME_LATER 强制 ackIndex=-1，listener 写的宽 ackIndex 无效
        var h = new Harness();
        Assert.False(h.Run(OffsetBatch(3), ConsumeConcurrentlyStatus.ReconsumeLater, 1));
        Assert.Equal(new List<(long, int)> { (0, 1), (1, 1), (2, 1) }, h.Pending());
        Assert.Null(h.Offset());
    }

    [Fact]
    public void ListenerExceptionIsTreatedAsReconsumeLater()
    {
        // Java：listener 抛异常按 RECONSUME_LATER 处理，整批回投
        var h = new Harness();
        h.Run(OffsetBatch(2), ConsumeConcurrentlyStatus.ConsumeSuccess, 5, @throw: true);
        Assert.Equal(new List<(long, int)> { (0, 1), (1, 1) }, h.Pending());
        Assert.Null(h.Offset());
    }

    [Fact]
    public void BroadcastingDropsTheTailWithoutSendBack()
    {
        // 广播模式没有 %RETRY% 可回投：未认可的尾巴只 warn 就丢掉，整批位点前进
        //（Java:232-237，重启后不重投）
        var h = new Harness(MessageModel.Broadcasting);
        Assert.True(h.Run(OffsetBatch(3), ConsumeConcurrentlyStatus.ConsumeSuccess, 0));
        Assert.Empty(h.Pending());
        Assert.Equal(3, h.Offset());
        Assert.Equal(3, h.Consumed());
    }

    [Fact]
    public void BroadcastingReconsumeLaterStillAdvances()
    {
        var h = new Harness(MessageModel.Broadcasting);
        Assert.True(h.Run(OffsetBatch(2), ConsumeConcurrentlyStatus.ReconsumeLater, null));
        Assert.Equal(2, h.Offset());
    }
}
