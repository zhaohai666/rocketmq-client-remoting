// OFFSET_ILLEGAL 纠错分支单测（Java DefaultMQPushConsumerImpl:402-427），不需要集群。
//
// 为什么必须离线锁死：这条路径错了是**静默**的两种极端 ——
//
//   - 只把拉取游标拨到 nextBeginOffset 而不丢队列/不冻结：broker 刚把位点纠正到合法区间，
//     这条队列上**已经取回还没 ack** 的旧批次一 ack 又把位点推回非法值，下一轮拉取再被
//     broker 拒一次 —— 客户端与 broker 之间来回弹跳，永不停歇；
//   - 纠错后的位点没立刻落盘：进程在下一轮周期落盘（默认 5s）之前崩掉，broker 上留着的还是
//     非法位点，重启后从非法位点起拉 —— 这条纠错等于没做。
//
// 两个方向在真机短期窗口里都看不出差别（消息照消费、只是一直在弹/一次崩溃才暴露），
// 所以断言全部放离线；真机另有一条链路证明（examples 的 live-offset-illegal 子命令）。
//
// 判据来源：PullMessageProcessor 对 OFFSET_OVERFLOW_BADLY / OFFSET_TOO_SMALL / OFFSET_RESET
// 一律回 PULL_OFFSET_MOVED，MQClientAPIImpl:1099 把它映射成 PullStatus.OffsetIllegal，修正值
// 在应答头 nextBeginOffset。Java 的处理是 setNextOffset → ProcessQueue.setDropped(true) → 异步
// { updateAndFreezeOffset; persist; removeProcessQueue; rebalanceImmediately }。
//
// 与 python/tests/test_offset_illegal_recover.py、rust/src/client/consumer.rs、
// cpp/tests/test_offset_illegal_recover.cpp 的同名用例逐条对拍。
//
// C# 侧的实现差异（不是漏掉，是刻意为之，见 Consumer.cs 注释）：没有 per-queue 的 classic
// ProcessQueue 对象，"这条队列还在不在我名下"用 _pullThreads 引用相等判；网络段（persist 发给
// broker）离线不可观测，此处断言"纠正值确实交给了持久化路径"（RetiredQueuesForTest 里那条
// 记录的 ConsumeOffset/HadOffset 就是会走 UpdateConsumerOffset 的材料），线上那一步由真机脚本
// 用 broker 侧位点回读证明。
using RocketMQ.Client;
using RocketMQ.Common;
using Xunit;

namespace RocketMQ.Client.Tests;

public class OffsetIllegalRecoverTests
{
    private const string Group = "GID_OffsetIllegalNetUnit";
    private const string Topic = "OffsetIllegalNetUnitTopic";
    private const string Broker = "broker-a";

    private static MessageQueue Queue0() => new(Topic, Broker, 0);

    private static MessageExt Ext(long queueOffset) => new()
    {
        Topic = Topic,
        BrokerName = Broker,
        QueueId = 0,
        QueueOffset = queueOffset,
    };

    private static List<MessageExt> OffsetBatch(params int[] offsets)
        => offsets.Select(o => Ext(o)).ToList();

    /// <summary>只数调用次数：用于「排队期间被丢弃的批次连 listener 都不该进」。</summary>
    private sealed class CountingListener : IMessageListenerConcurrently
    {
        public List<List<MessageExt>> Calls { get; } = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
                                                       ConsumeConcurrentlyContext context)
        {
            Calls.Add(new List<MessageExt>(msgs));
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    /// <summary>一个不碰网络的 push consumer：缓冲/位点/代号都手动搭。</summary>
    private sealed class Harness
    {
        public DefaultMQPushConsumer C { get; }
        public MessageQueue Mq { get; } = Queue0();
        public string Key { get; }

        public Harness(bool assigned = true, long epoch = 0)
        {
            C = new DefaultMQPushConsumer(Group) { ConsumeMessageBatchMaxSize = 2 };
            Key = DefaultMQPushConsumer.OffsetKeyForTest(Mq);
            if (assigned)
            {
                C.SetAssignedForTest(Key, Mq);
                C.RegisterLoopForTest(Key, alive: false);
                C.SetConsumeOffsetForTest(Key, 3);
                C.SetQueueEpochForTest(Key, epoch);
            }
        }

        /// <summary>模拟拉取回调里 OFFSET_ILLEGAL 的锁内动作（游标 + 冻结 + 修正值）。</summary>
        public void Freeze(long off) => C.FreezeOffsetForIllegalForTest(Key, off);

        public void Recover() => C.OffsetIllegalRecoverForTest(Key, Mq);

        public void Ack(int[] offsets, long? epoch = null)
            => C.AdvanceConsumeOffsetForTest(Key, OffsetBatch(offsets), null, epoch);

        public long? Offset() => C.ConsumeOffsetForTest(Key);
    }

    // ------------------------------------------------- 纠错 = 冻结 + 丢队列 + 立刻落盘

    /// <summary>队列的本地状态全部作废：缓冲、位点、线程登记、分配登记。</summary>
    [Fact]
    public void Recover_DropsEverythingForTheQueue()
    {
        var h = new Harness();
        h.C.SetPendingForTest(h.Key, OffsetBatch(1, 2));
        Assert.Equal(2, h.C.TakeBatchForConsume(h.Key, out _).Count);
        Assert.Equal(1, h.C.InFlightForTest(h.Key));
        h.Freeze(0);

        h.Recover();

        Assert.Empty(h.C.PendingForTest(h.Key));
        Assert.Null(h.Offset());
        Assert.False(h.C.HasPullLoop(h.Key));
        Assert.Equal(-1L, h.C.LastPullAt(h.Key));
        Assert.False(h.C.MqMapContainsForTest(h.Key));
    }

    /// <summary>
    /// Java 的显式 persist：不等周期落盘，纠错当场写回 broker。离线只能断言"交给持久化路径的
    /// 材料"（记录里的 ConsumeOffset/HadOffset）；真机上 broker 侧位点由非法值回到修正值，
    /// 见 examples 的 live-offset-illegal。
    /// </summary>
    [Fact]
    public void Recover_HandsTheCorrectedOffsetToPersist()
    {
        var h = new Harness();
        h.Freeze(0);

        h.Recover();

        DefaultMQPushConsumer.RetiredForTest record = Assert.Single(h.C.RetiredQueuesForTest());
        Assert.True(record.HadOffset);
        Assert.Equal(0L, record.ConsumeOffset);
        Assert.Equal(h.Mq, record.Mq);
    }

    /// <summary>removeProcessQueue 之后靠 rebalanceImmediately 重建这条队列。</summary>
    [Fact]
    public void Recover_WakesTheRebalanceLoop()
    {
        var h = new Harness();
        h.Freeze(0);
        Assert.False(h.C.RebalancePendingForTest());

        h.Recover();

        Assert.True(h.C.RebalancePendingForTest());
    }

    /// <summary>代号 +1 是「旧批次作废」的依据（Java setDropped(true)）。</summary>
    [Fact]
    public void Recover_BumpsTheQueueEpoch()
    {
        var h = new Harness(epoch: 0);
        h.Freeze(0);

        h.Recover();

        Assert.Equal(1L, h.C.QueueEpochForTest(h.Key));
    }

    /// <summary>
    /// 队列已经不在本实例名下（并发撤销后又轮到这条回调）：没有可落盘的位点就**不能**凭空造一条
    /// ——写一个 0 会把 broker 上别的实例推进的位点抹掉。重建请求照样发（Java 的
    /// rebalanceImmediately 无条件）。
    /// </summary>
    [Fact]
    public void Recover_WithoutAConsumedOffset_DoesNotInventOne()
    {
        var h = new Harness(assigned: false);

        h.Recover();

        DefaultMQPushConsumer.RetiredForTest record = Assert.Single(h.C.RetiredQueuesForTest());
        Assert.False(record.HadOffset);
        Assert.True(h.C.RebalancePendingForTest());
    }

    // ------------------------------------------------- 冻结：修正值不被在途 ack 推翻

    /// <summary>旧批次（offset 0..2 已消费）迟到的 ack：把位点推回 3 就是 Java 的弹跳现场。</summary>
    [Fact]
    public void FrozenOffset_IgnoresAck()
    {
        var h = new Harness();
        h.Freeze(0);

        h.Ack(new[] { 0, 1, 2 }, epoch: 0);

        Assert.Equal(0L, h.Offset());
    }

    /// <summary>被冻结的位点同样不许被 correctTagsOffset 抬走（Java allowToUpdate=false）。</summary>
    [Fact]
    public void FrozenOffset_IgnoresCorrectTagsOffset()
    {
        var h = new Harness();
        h.Freeze(0);

        h.C.CorrectTagsOffsetForTest(h.Key, PullStatus.NoNewMsg, 110);

        Assert.Equal(0L, h.Offset());
    }

    /// <summary>
    /// 有意偏差：Java 在 removeOffset 时解冻、靠 ProcessQueue.isDropped() 兜底；本端口没有
    /// per-batch 的 ProcessQueue 对象，冻结一直留到队列重建（更严）。
    /// </summary>
    [Fact]
    public void FreezeSurvivesTheDrop_UntilTheQueueIsRebuilt()
    {
        var h = new Harness();
        h.Freeze(0);

        h.Recover();

        Assert.True(h.C.OffsetFrozenForTest(h.Key));
    }

    /// <summary>重建（新一轮 rebalance 把队列再划给自己、新循环就位）必须解冻，否则这条队列
    /// 从此只拉不 ack —— 位点永久停在纠错值。</summary>
    [Fact]
    public void Rebuild_ClearsTheFreeze()
    {
        var h = new Harness();
        h.Freeze(0);
        h.Recover();
        h.C.ClearRetiredForTest();

        h.C.SetStartedForTest(true);
        h.C.RebuildPullThreadsForTest(new List<MessageQueue> { h.Mq });

        Assert.False(h.C.OffsetFrozenForTest(h.Key));
        Assert.Equal(1L, h.C.QueueEpochForTest(h.Key));
        // 重建后的新代号 ack 恢复正常；旧代号（0）递到的 ack 依旧不许动位点
        h.Ack(new[] { 0, 1 }, epoch: 0);
        Assert.Null(h.Offset());
        h.Ack(new[] { 0, 1 }, epoch: 1);
        Assert.Equal(2L, h.Offset());
    }

    // ------------------------------------------------- 旧代号的批次整批作废（Java :267/:339）

    /// <summary>队列被撤销/重建（代号 0 → 1）后，旧批次迟到的 ack 不能再动位点。</summary>
    [Fact]
    public void StaleEpochAck_IsDropped()
    {
        var h = new Harness(epoch: 1);
        h.C.SetConsumeOffsetForTest(h.Key, 0);

        h.Ack(new[] { 0, 1, 2 }, epoch: 0);

        Assert.Equal(0L, h.Offset());
    }

    [Fact]
    public void CurrentEpochAck_Advances()
    {
        var h = new Harness(epoch: 1);
        h.C.SetConsumeOffsetForTest(h.Key, 0);

        h.Ack(new[] { 0, 1, 2 }, epoch: 1);

        Assert.Equal(3L, h.Offset());
    }

    /// <summary>Java ConsumeMessageConcurrentlyService:339 —— 排队期间被丢弃的批次连 listener
    /// 都不该进（消息已由新属主/重建后的队列接管）。</summary>
    [Fact]
    public void StaleEpochBatch_IsNotConsumed()
    {
        var h = new Harness(epoch: 1);
        var listener = new CountingListener();
        h.C.SetMessageListener(listener);

        bool done = h.C.ConsumeBatchForTest(h.Key, h.Mq, OffsetBatch(0, 1), epoch: 0);

        Assert.False(done);
        Assert.Empty(listener.Calls);
    }

    [Fact]
    public void CurrentEpochBatch_IsConsumed()
    {
        var h = new Harness(epoch: 1);
        var listener = new CountingListener();
        h.C.SetMessageListener(listener);
        h.C.SetConsumeOffsetForTest(h.Key, 0);

        bool done = h.C.ConsumeBatchForTest(h.Key, h.Mq, OffsetBatch(0, 1), epoch: 1);

        Assert.True(done);
        Assert.Single(listener.Calls);
        Assert.Equal(2L, h.Offset());
    }
}
