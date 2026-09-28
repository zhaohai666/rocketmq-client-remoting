// cleanExpiredMsg（Java ConsumeMessageConcurrentlyService:68-88 建调度、:192-200 清扫入口
// ＋ ProcessQueue.cleanExpiredMsg:75-127）单测 —— 不需要集群。
//
// 为什么必须离线锁死：这是唯一能把**卡死的 listener** 救出来的闸门，坏了全是静默的：
//   - 判松了（不严格大于、没盖章也算过期、对非队首动手）：正在被消费的消息被抢走回投，
//     重复投递；
//   - 判紧了（队首不过期却继续扫后面）：队首永远卡住，后面的过期消息永远出不去；
//   - 上限写丢（单轮无界）：一次把整条队列清空，位点/重投节奏全乱；
//   - 回投失败后摘除：消息真丢。
// 未 Start 的消费者 SendMessageBack 必定抛，所以离线能锁死「判定」与「回投失败原地保留」
// 两条分支；真机链路（挂起 → 清扫 → %RETRY% 重投到达）见
// examples/RocketMQ.Examples/LiveCleanExpiredMsg.cs。与 python/tests/test_clean_expired_msg.py、
// cpp/tests/test_clean_expired_msg.cpp、rust/src/client/consumer.rs 的同名测试一一对应。

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class CleanExpiredMsgTests
{
    private const string Group = "GID_CleanExpiredNetUnit";
    private const string Topic = "CleanExpiredNetUnitTopic";
    private const string Broker = "broker-a";

    private static MessageQueue Queue0() => new(Topic, Broker, 0);

    private static MessageExt Ext(long queueOffset, long? stampMs = null)
    {
        var m = new MessageExt
        {
            Topic = Topic,
            BrokerName = Broker,
            QueueId = 0,
            QueueOffset = queueOffset,
        };
        if (stampMs.HasValue)
        {
            m.PutProperty(MessageConst.PropertyConsumeStartTimestamp,
                stampMs.Value.ToString(System.Globalization.CultureInfo.InvariantCulture));
        }

        return m;
    }

    private static long Now() => UtilAll.CurrentTimeMillis();

    /// <summary>未 Start 的消费者：SendMessageBack 必定抛（正是「回投失败原地保留」的夹具）。</summary>
    private sealed class Harness
    {
        private readonly DefaultMQPushConsumer _consumer;

        public Harness(int consumeTimeout = 1)
        {
            _consumer = new DefaultMQPushConsumer(Group) { ConsumeTimeout = consumeTimeout };
            Key = DefaultMQPushConsumer.OffsetKeyForTest(Queue0());
        }

        public string Key { get; }

        public DefaultMQPushConsumer Consumer => _consumer;

        public void Seed(params MessageExt[] msgs)
            => _consumer.SetPendingForTest(Key, msgs.ToList());

        public int Clean() => _consumer.CleanExpiredQueueForTest(Key);

        public List<MessageExt> Entries() => _consumer.ProcessQueueEntriesForTest(Key);

        public long? Offset() => _consumer.ConsumeOffsetForTest(Key);
    }

    /// <summary>按 Java listener 契约回投尾巴。</summary>
    private sealed class ReconsumeListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
                                                       ConsumeConcurrentlyContext context)
            => ConsumeConcurrentlyStatus.ReconsumeLater;
    }

    private sealed class NoopOrderlyListener : IMessageListenerOrderly
    {
        public bool Orderly() => true;

        public ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context)
            => ConsumeOrderlyStatus.Success;
    }

    /// <summary>Java scheduleAtFixedRate(cleanExpireMsg, consumeTimeout, consumeTimeout, MINUTES)：
    /// initialDelay 与 period 同值，都是 consumeTimeout 分钟。</summary>
    [Fact]
    public void PeriodEqualsConsumeTimeoutMinutes()
    {
        Assert.Equal(900_000L, new Harness(15).Consumer.CleanExpirePeriodMillisForTest());
        Assert.Equal(60_000L, new Harness(1).Consumer.CleanExpirePeriodMillisForTest());
        Assert.Equal(60_000L, new Harness(0).Consumer.CleanExpirePeriodMillisForTest());
        Assert.Equal(60_000L, new Harness(-3).Consumer.CleanExpirePeriodMillisForTest());
    }

    /// <summary>盖章解析对齐 Java 的 StringUtils.isNotEmpty(...) &amp;&amp; Long.parseLong(...)：
    /// 没有/空/坏值一律按「没盖过章」处理，不当成过期（判松了会抢走刚投递的消息）。</summary>
    [Fact]
    public void ConsumeStartTimestampReadsOnlyWellFormedStamps()
    {
        MessageExt m = Ext(0);
        Assert.Null(DefaultMQPushConsumer.ConsumeStartTimestampForTest(m));
        m.PutProperty(MessageConst.PropertyConsumeStartTimestamp, string.Empty);
        Assert.Null(DefaultMQPushConsumer.ConsumeStartTimestampForTest(m));
        m.PutProperty(MessageConst.PropertyConsumeStartTimestamp, "not-a-number");
        Assert.Null(DefaultMQPushConsumer.ConsumeStartTimestampForTest(m));
        m.PutProperty(MessageConst.PropertyConsumeStartTimestamp, "1700000000000");
        Assert.Equal(1_700_000_000_000L, DefaultMQPushConsumer.ConsumeStartTimestampForTest(m));
    }

    /// <summary>过期判据是**严格**大于（等于阈值不动手），没盖章不当过期。</summary>
    [Fact]
    public void IsConsumeExpiredIsStrictAndIgnoresMissingStamps()
    {
        const long now = 10_000_000L;
        Assert.True(DefaultMQPushConsumer.IsConsumeExpiredForTest(now - 60_001, now, 1));
        Assert.False(DefaultMQPushConsumer.IsConsumeExpiredForTest(now - 60_000, now, 1));
        Assert.False(DefaultMQPushConsumer.IsConsumeExpiredForTest(now, now, 1));
        Assert.False(DefaultMQPushConsumer.IsConsumeExpiredForTest(null, now, 1));
        // 默认 15 分钟：14 分 59.999 秒不动手，15 分零 1 毫秒动手
        Assert.False(DefaultMQPushConsumer.IsConsumeExpiredForTest(now - 899_999, now, 15));
        Assert.True(DefaultMQPushConsumer.IsConsumeExpiredForTest(now - 900_001, now, 15));
    }

    /// <summary>队首没过期 → 整条队列都不动（Java:97-99 的 break，不是 continue）。</summary>
    [Fact]
    public void StopsAtAFreshHead()
    {
        var h = new Harness();
        h.Seed(Ext(0), Ext(1, Now() - 120_000));  // 头没盖章，尾巴过期
        Assert.Equal(0, h.Clean());
        Assert.Equal(2, h.Entries().Count);
    }

    /// <summary>没有盖章的消息即便在队首也绝不回投（Java StringUtils.isNotEmpty 短路）。</summary>
    [Fact]
    public void LeavesUnsignedMessagesAlone()
    {
        var h = new Harness();
        h.Seed(Ext(0));
        Assert.Equal(0, h.Clean());
        Assert.Single(h.Entries());
    }

    /// <summary>坏戳与空戳同命：坏值解析失败按没盖章处理，不是「等同于 0」。</summary>
    [Fact]
    public void BadStampIsNotTreatedAsAncient()
    {
        var h = new Harness();
        MessageExt m = Ext(0);
        m.PutProperty(MessageConst.PropertyConsumeStartTimestamp, "1700000000000x");
        h.Seed(m);
        Assert.Equal(0, h.Clean());
        Assert.Single(h.Entries());
    }

    /// <summary>回投失败（未 start 的消费者）→ 条目原地保留；单轮内 Java 会对**同一条队首**
    /// 重试到 loop 用尽（:80 的 loop 与循环体里的 continue）。</summary>
    [Fact]
    public void KeepsTheEntryWhenSendBackFails()
    {
        var h = new Harness();
        h.Seed(Ext(0, Now() - 120_000), Ext(1, Now() - 120_000));
        Assert.Equal(2, h.Clean());  // loop=min(2,16)=2：同一条队首重试两次
        Assert.Equal(2, h.Entries().Count);
        Assert.Null(h.Offset());
    }

    /// <summary>单轮上限 16：20 条过期条目一轮只发起 16 次回投（Java:80 的 loop 在循环之前算一次）。</summary>
    [Fact]
    public void CapsOneRoundAtSixteen()
    {
        var h = new Harness();
        long stamp = Now() - 120_000;
        h.Seed(Enumerable.Range(0, 20).Select(i => Ext(i, stamp)).ToArray());
        Assert.Equal(16, h.Clean());
        Assert.Equal(20, h.Entries().Count);
    }

    /// <summary>顺序消费没有这条路径（Java ProcessQueue:76-78 直接早退）。</summary>
    [Fact]
    public void SkipsOrderlyConsumers()
    {
        var h = new Harness();
        h.Consumer.SetMessageListener(new NoopOrderlyListener());
        h.Seed(Ext(0, Now() - 120_000));
        Assert.Equal(0, h.Clean());
        Assert.Single(h.Entries());
    }

    /// <summary>「仍是队首才摘」的两种让位：正常收尾（条目已摘/已前进）与更小位点冒头。</summary>
    [Fact]
    public void RemoveExpiredEntryOnlyWhenStillHead()
    {
        // 仍是队首：视图里摘掉
        var h = new Harness();
        h.Consumer.SetPendingForTest(h.Key, new List<MessageExt> { Ext(0), Ext(1) });
        Assert.True(h.Consumer.RemoveExpiredEntryIfStillHeadForTest(h.Key, h.Entries().Single(m => m.QueueOffset == 0)));
        Assert.Single(h.Entries());

        // 前面冒出了更小的位点：让位，不许抢摘
        var h2 = new Harness();
        h2.Consumer.SetPendingForTest(h2.Key, new List<MessageExt> { Ext(3), Ext(5) });
        MessageExt five = h2.Entries().Single(m => m.QueueOffset == 5);
        Assert.False(h2.Consumer.RemoveExpiredEntryIfStillHeadForTest(h2.Key, five));
        Assert.Equal(2, h2.Entries().Count);

        // 已经不在视图里（listener 正常收尾先摘了）：同样为 false，且不抛
        var h3 = new Harness();
        Assert.False(h3.Consumer.RemoveExpiredEntryIfStillHeadForTest(h3.Key, Ext(0)));
    }

    /// <summary>在册视图 = 在途（已分发未落定）∪ 已拉未分发，即 Java msgTreeMap；
    /// 取批后立刻可见，收尾后立刻不可见。</summary>
    [Fact]
    public void ProcessQueueViewSeesInFlightAndPending()
    {
        var h = new Harness();
        h.Consumer.ConsumeMessageBatchMaxSize = 2;
        h.Seed(Ext(0), Ext(1), Ext(2));
        List<MessageExt> batch = h.Consumer.TakeBatchForConsume(h.Key);
        Assert.Equal(2, batch.Count);
        Assert.Equal(3, h.Entries().Count);
        h.Consumer.FinishBatchConsume(h.Key, batch);
        Assert.Single(h.Entries());
    }

    /// <summary>回投要跳过已被 cleanExpiredMsg 清扫的条目（Java processConsumeResult:243-248）：
    /// 它已经在回 broker 的路上，再发一次就是重复消息。未 start 的消费者里，只有未被清扫的
    /// 那条才会走到「回投失败 → 计数 +1 并塞回队首」的落点。</summary>
    [Fact]
    public void SendBackSkipsEntriesSweptAway()
    {
        var h = new Harness();
        h.Consumer.ConsumeMessageBatchMaxSize = 2;
        h.Consumer.SetMessageListener(new ReconsumeListener());
        h.Seed(Ext(0), Ext(1));
        List<MessageExt> batch = h.Consumer.TakeBatchForConsume(h.Key);
        Assert.Equal(2, batch.Count);

        // 模拟清扫先把本批的第一条回投并摘除（它在 _inFlightMsgs 里，两处一起清）
        h.Consumer.RemoveProcessQueueEntryForTest(h.Key, batch[0]);

        h.Consumer.ConsumeBatchForTest(h.Key, Queue0(), batch);

        Assert.Equal(0, batch[0].ReconsumeTimes);
        Assert.Equal(1, batch[1].ReconsumeTimes);
        List<MessageExt> pending = h.Consumer.PendingForTest(h.Key);
        Assert.Single(pending);
        Assert.Same(batch[1], pending[0]);
    }
}
