// correctTagsOffset（Java DefaultMQPushConsumerImpl:713-717，调用点 :394-401）单测 —— 不需要集群。
//
// 为什么必须离线锁死：拉取应答是 NO_NEW_MSG / NO_MATCHED_MSG 时，若 ProcessQueue 里没有消息，
// Java 会把"已消费位点"抬到应答的 nextBeginOffset（increaseOnly=true）。漏了这件事是**静默**的：
// broker 侧按 tags/表达式把这一段的每一条都滤掉时，客户端既收不到消息、位点也不动，
// queryConsumerOffset 永远落后，重启后把这批没人要的消息从头再扫一遍。反过来，闸门写松
//（不等在途批次落定就抬位点）同样是静默的：进程崩溃时那批消息被跳过。
//
// 闸门 = Java 的 `0L == processQueue.getMsgCount()`：msgCount 数的是**仍在 ProcessQueue 里**的
// 消息，而并发消费的 removeMessage 要等 listener 返回才跑（ConsumeMessageConcurrentlyService:266），
// 所以在途批次也算数。本端口把 _pending 为空与 _inFlight 为 0 合成同一判据；DispatchLoop
// 的生产路径就是 TakeBatchForConsume + finally FinishBatchConsume（Consumer.cs:3304/3330），
// 所以这两个门面（连同 InFlightForTest）与 ConsumeBatchForTest 同一理由开放给单测。
// 与 python/tests/test_correct_tags_offset.py、rust/src/client/consumer.rs 的同名测试同题；
// 真机证据（broker 过滤 → NO_MATCHED_MSG → broker 侧位点前移）见 examples 的对应场景。

using RocketMQ.Client;
using RocketMQ.Common;

using Xunit;

namespace RocketMQ.Client.Tests;

public class CorrectTagsOffsetTests
{
    private const string Group = "GID_CorrectTagsOffsetNetUnit";
    private const string Topic = "CorrectTagsOffsetNetUnitTopic";
    private const string Broker = "broker-a";

    private static MessageQueue Queue0() => new(Topic, Broker, 0);

    private static MessageExt Ext(long queueOffset) => new()
    {
        Topic = Topic,
        BrokerName = Broker,
        QueueId = 0,
        QueueOffset = queueOffset,
    };

    private static List<MessageExt> OffsetBatch(int n)
        => Enumerable.Range(0, n).Select(i => Ext(i)).ToList();

    /// <summary>不 Start 的消费者即可：CorrectTagsOffset / Take / Finish 全在本地状态上。</summary>
    private sealed class Harness
    {
        private readonly DefaultMQPushConsumer _consumer;
        private readonly string _key;

        public Harness()
        {
            _consumer = new DefaultMQPushConsumer(Group) { ConsumeMessageBatchMaxSize = 2 };
            _key = DefaultMQPushConsumer.OffsetKeyForTest(Queue0());
        }

        public void SeedPending(List<MessageExt> msgs) => _consumer.SetPendingForTest(_key, msgs);

        public void Correct(PullStatus status, long nextOffset)
            => _consumer.CorrectTagsOffsetForTest(_key, status, nextOffset);

        public long? Offset() => _consumer.ConsumeOffsetForTest(_key);

        public List<MessageExt> Take() => _consumer.TakeBatchForConsume(_key);

        public void Finish() => _consumer.FinishBatchConsume(_key);

        public int InFlight() => _consumer.InFlightForTest(_key);

        public int PendingCount() => _consumer.PendingForTest(_key).Count;
    }

    /// <summary>空应答的两个状态（NO_NEW_MSG / NO_MATCHED_MSG）都推进；其余状态一律不碰位点。</summary>
    [Fact]
    public void AdvancesOnlyOnEmptyPullStatuses()
    {
        foreach (PullStatus s in new[] { PullStatus.NoNewMsg, PullStatus.NoMatchedMsg })
        {
            var h = new Harness();
            h.SeedPending(new List<MessageExt>());
            h.Correct(s, 42);
            Assert.Equal(42, h.Offset());
            h.Correct(s, 43);
            Assert.Equal(43, h.Offset());
        }

        foreach (PullStatus s in new[] { PullStatus.Found, PullStatus.OffsetIllegal })
        {
            var h = new Harness();
            h.SeedPending(new List<MessageExt>());
            h.Correct(s, 42);
            Assert.Null(h.Offset());
        }
    }

    /// <summary>Java 的 updateOffset(..., increaseOnly=true)：只前进不回退。</summary>
    [Fact]
    public void NeverRegresses()
    {
        var h = new Harness();
        h.Correct(PullStatus.NoNewMsg, 100);
        Assert.Equal(100, h.Offset());
        h.Correct(PullStatus.NoNewMsg, 50);
        Assert.Equal(100, h.Offset());
        h.Correct(PullStatus.NoNewMsg, 100);
        Assert.Equal(100, h.Offset());
        h.Correct(PullStatus.NoNewMsg, 101);
        Assert.Equal(101, h.Offset());
    }

    /// <summary>
    /// 表里没有记录时无条件建条目（Java <c>RemoteBrokerOffsetStore.updateOffset:61-64</c>
    /// 的 <c>putIfAbsent</c>），哪怕应答位点就是 0。
    /// 用 <c>TryGetValue</c> 的默认值 0 把「没有记录」与「记录是 0」混同，会让空队列
    /// （nextBeginOffset == 0）的首次修正变成静默 no-op：broker 上永远查不到这条队列的
    /// 位点记录（真机 S3 就是这么抓出来的）。
    /// </summary>
    [Fact]
    public void CreatesTheRecordEvenForAZeroOffset()
    {
        var h = new Harness();
        h.SeedPending(new List<MessageExt>());
        h.Correct(PullStatus.NoNewMsg, 0);
        Assert.Equal(0, h.Offset());
        h.Correct(PullStatus.NoNewMsg, 0);
        Assert.Equal(0, h.Offset());
    }

    /// <summary>ProcessQueue 里还有消息（缓冲里的 + listener 手里的）就一律不动位点。</summary>
    [Fact]
    public void RespectsTheProcessQueueGuard()
    {
        // 缓冲里还有没消费的
        var h = new Harness();
        h.SeedPending(new List<MessageExt> { Ext(0) });
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Null(h.Offset());

        // 缓冲空了，但有一条在 listener 手里（removeMessage 还没跑）
        h.Take();
        Assert.Equal(1, h.InFlight());
        h.Correct(PullStatus.NoMatchedMsg, 42);
        Assert.Null(h.Offset());

        // 消费收尾（含异常回塞路径）后计数归零，修正才允许生效
        h.Finish();
        Assert.Equal(0, h.InFlight());
        h.Correct(PullStatus.NoMatchedMsg, 42);
        Assert.Equal(42, h.Offset());
    }

    /// <summary>连 pending 表项都没有（从未拉过该队列）按空处理，不能因为缺项漏掉修正。</summary>
    [Fact]
    public void MissingPendingEntryAllowsCorrection()
    {
        var h = new Harness();
        h.Correct(PullStatus.NoNewMsg, 7);
        Assert.Equal(7, h.Offset());
    }

    /// <summary>取批次必须与登记在途同一临界区：取走后闸门立刻关上。</summary>
    [Fact]
    public void TakeRegistersInFlightInTheSameCriticalSection()
    {
        var h = new Harness();
        h.SeedPending(OffsetBatch(3));

        List<MessageExt> batch = h.Take();
        Assert.Equal(2, batch.Count);
        Assert.Equal(1, h.InFlight());
        Assert.Equal(1, h.PendingCount());
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Null(h.Offset());

        // 还有余量没被取走：缓冲非空，同样挡着
        h.Finish();
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Null(h.Offset());

        // 第二批发完，闸门全开
        Assert.Single(h.Take());
        h.Finish();
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Equal(42, h.Offset());
    }

    /// <summary>两批同时在途（两个消费线程）计数必须累加；清零后又被擦除的 key 要能重新登记。</summary>
    [Fact]
    public void InFlightCountsAccumulateAndCanBeReRegistered()
    {
        var h = new Harness();
        h.SeedPending(OffsetBatch(4));
        h.Take();
        h.Take();
        Assert.Equal(2, h.InFlight());
        h.Finish();
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Null(h.Offset());
        h.Finish();
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Equal(42, h.Offset());

        var h2 = new Harness();
        h2.SeedPending(OffsetBatch(3));
        h2.Take();
        h2.Finish();
        Assert.Equal(0, h2.InFlight());
        h2.Take();
        Assert.Equal(1, h2.InFlight());
        h2.Correct(PullStatus.NoNewMsg, 42);
        Assert.Null(h2.Offset());
        h2.Finish();
        h2.Correct(PullStatus.NoNewMsg, 42);
        Assert.Equal(42, h2.Offset());
    }

    /// <summary>闸门不能被自己的记账弄死：空队列 take 不登记，重复 finish 不复活计数。</summary>
    [Fact]
    public void BookkeepingIsIdempotentAndLeakFree()
    {
        var h = new Harness();
        h.SeedPending(new List<MessageExt>());
        Assert.Empty(h.Take());
        Assert.Equal(0, h.InFlight());
        h.Correct(PullStatus.NoNewMsg, 42);
        Assert.Equal(42, h.Offset());

        var h2 = new Harness();
        h2.SeedPending(OffsetBatch(1));
        h2.Take();
        h2.Finish();
        h2.Finish();
        Assert.Equal(0, h2.InFlight());
        h2.Correct(PullStatus.NoNewMsg, 42);
        Assert.Equal(42, h2.Offset());
    }
}
