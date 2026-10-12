// ProduceAccumulator（自动攒批）的离线单测 —— 与 python/tests/test_produce_accumulator.py 同题。
using System.Globalization;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;

namespace RocketMQ.Client.Tests;

public class ProduceAccumulatorTests
{
    private const string Topic = "AccumTestTopic";

    // ---------------------------------------------------------------- 夹具

    private sealed class FakeProducer : DefaultMQProducer
    {
        private readonly object _gate = new();
        private readonly SendResult? _canned;

        public FakeProducer(SendResult? canned = null) : base("PG_AccumFake") => _canned = canned;

        public List<(Message Msg, MessageQueue? Mq, ISendCallback? Callback)> Sent { get; } = new();

        public override SendResult? SendDirect(Message msg, MessageQueue? mq, ISendCallback? sendCallback)
        {
            lock (_gate)
            {
                Sent.Add((msg, mq, sendCallback));
            }

            SendResult result = _canned ?? new SendResult
            {
                SendStatus = SendStatus.SendOk,
                MsgId = "123",
                MessageQueue = mq ?? new MessageQueue(),
                QueueOffset = 0,
            };
            sendCallback?.OnSuccess(result);
            return result;
        }
    }

    private sealed class CollectingCallback : ISendCallback
    {
        private readonly object _gate = new();
        private readonly List<SendResult> _results = new();
        private readonly List<Exception> _errors = new();

        public void OnSuccess(SendResult sendResult)
        {
            lock (_gate)
            {
                _results.Add(sendResult);
            }
        }

        public void OnException(Exception e)
        {
            lock (_gate)
            {
                _errors.Add(e);
            }
        }

        public List<SendResult> Results
        {
            get
            {
                lock (_gate)
                {
                    return new List<SendResult>(_results);
                }
            }
        }

        public List<Exception> Errors
        {
            get
            {
                lock (_gate)
                {
                    return new List<Exception>(_errors);
                }
            }
        }

        public int Total
        {
            get
            {
                lock (_gate)
                {
                    return _results.Count + _errors.Count;
                }
            }
        }
    }

    private static List<Message> MakeMessages(int n = 5)
    {
        var messages = new List<Message>();
        for (int i = 0; i < n; ++i)
        {
            var msg = new Message(Topic, Encoding.UTF8.GetBytes(new string('1', i + 1)));
            MessageClientIDSetter.SetUniqId(msg);
            messages.Add(msg);
        }

        return messages;
    }

    private static byte[] ReferenceBatchBody(List<Message> messages) =>
        MessageBatch.GenerateFromList(messages).Encode();

    private static bool WaitUntil(Func<bool> predicate, double timeoutSeconds = 5.0)
    {
        var clock = System.Diagnostics.Stopwatch.StartNew();
        while (clock.Elapsed.TotalSeconds < timeoutSeconds)
        {
            if (predicate())
            {
                return true;
            }

            Thread.Sleep(10);
        }

        return predicate();
    }

    /// <summary>起一根 **后台**线程。⚠ 必须后台：不少用例是"多条消息并发 add、攒在同一批里、
    /// 由主线程手动触发发送"，一旦断言提前失败，还阻塞在 Monitor.Wait 里的工作线程就会吊住
    /// 测试进程。</summary>
    private static Thread StartWorker(Action action)
    {
        var thread = new Thread(() => action()) { IsBackground = true };
        thread.Start();
        return thread;
    }

    // ---------------------------------------------------------------- 参数

    [Fact]
    public void DefaultParams_MatchLegacy()
    {
        var acc = new ProduceAccumulator("params");
        Assert.Equal(10, acc.HoldMs);
        Assert.Equal(32 * 1024, acc.HoldSize);
        Assert.Equal(32L * 1024 * 1024, acc.TotalHoldSize);
        Assert.Equal(32 * 1024, acc.GetTotalBatchMaxBytes());
        Assert.Equal(0, acc.CurrentlyHoldSize);
    }

    [Fact]
    public void ParamGuards_CopyLegacyRanges()
    {
        var acc = new ProduceAccumulator("guards");
        acc.BatchMaxDelayMs(1);
        acc.BatchMaxDelayMs(30 * 1000);
        Assert.Throws<ArgumentException>(() => acc.BatchMaxDelayMs(0));
        Assert.Throws<ArgumentException>(() => acc.BatchMaxDelayMs(30 * 1000 + 1));

        acc.BatchMaxBytes(1);
        acc.BatchMaxBytes(2 * 1024 * 1024);
        Assert.Throws<ArgumentException>(() => acc.BatchMaxBytes(0));
        Assert.Throws<ArgumentException>(() => acc.BatchMaxBytes(2 * 1024 * 1024 + 1));

        acc.TotalBatchMaxBytes(1);
        Assert.Throws<ArgumentException>(() => acc.TotalBatchMaxBytes(0));
    }

    [Fact]
    public void ProducerGetters_ReturnZeroBeforeAccumulatorExists()
    {
        var producer = new DefaultMQProducer("PG_AccumGetter");
        Assert.Equal(0, producer.BatchMaxDelayMs);
        Assert.Equal(0, producer.BatchMaxBytes);
        Assert.Equal(0, producer.TotalBatchMaxBytes);
        Assert.False(producer.AutoBatch);
        producer.AutoBatch = true;
        Assert.False(producer.AutoBatch);
    }

    [Fact]
    public void InitProduceAccumulator_SyncsThresholdsAndReusesByClientId()
    {
        string clientId = "accum-reuse-" + Guid.NewGuid().ToString("N");
        var producer = new DefaultMQProducer("PG_AccumInit") { ClientId = clientId };
        producer.BatchMaxDelayMs = 50;
        producer.BatchMaxBytes = 1024;
        producer.TotalBatchMaxBytes = 4096;
        producer.InitProduceAccumulator();

        ProduceAccumulator acc = producer.ProduceAccumulator!;
        Assert.Same(ProduceAccumulatorRegistry.GetOrCreate(clientId), acc); // 按 clientId 复用
        Assert.Equal(50, acc.GetBatchMaxDelayMs());
        Assert.Equal(1024, acc.GetBatchMaxBytes());
        Assert.Equal(4096, acc.TotalHoldSize);
        producer.BatchMaxDelayMs = 80;
        Assert.Equal(80, acc.GetBatchMaxDelayMs());
        // 再来一个同 clientId 的 producer → 复用同一个累加器
        var second = new DefaultMQProducer("PG_AccumInit2") { ClientId = clientId };
        second.InitProduceAccumulator();
        Assert.Same(acc, second.ProduceAccumulator);
    }

    [Fact]
    public void TryAddMessage_GateAndRelease()
    {
        var acc = new ProduceAccumulator("gate");
        acc.TotalBatchMaxBytes(10);
        var ten = new Message(Topic, Encoding.ASCII.GetBytes("1234567890"));
        Assert.True(acc.TryAddMessage(ten));
        Assert.Equal(10, acc.CurrentlyHoldSize);
        Assert.False(acc.TryAddMessage(new Message(Topic, Encoding.ASCII.GetBytes("x"))));
        Assert.Equal(10, acc.CurrentlyHoldSize);
        acc.ReleaseHold(10);
        Assert.Equal(0, acc.CurrentlyHoldSize);
        Assert.True(acc.TryAddMessage(new Message(Topic, Encoding.ASCII.GetBytes("x"))));
        // 空 body 不记账但仍然放行
        acc.ReleaseHold(1);
        acc.TotalBatchMaxBytes(5);
        Assert.True(acc.TryAddMessage(new Message(Topic, Array.Empty<byte>())));
        Assert.Equal(0, acc.CurrentlyHoldSize);
    }

    // ---------------------------------------------------------------- 归并

    [Fact]
    public void SyncBatch_MergesMessagesAndReturnsPerMessageResults()
    {
        var canned = new SendResult
        {
            SendStatus = SendStatus.SendOk,
            MsgId = "id-0,id-1,id-2,id-3,id-4",
            MessageQueue = new MessageQueue(Topic, "broker-a", 0),
            QueueOffset = 100,
            OffsetMsgId = "off-0,off-1,off-2,off-3,off-4",
        };
        var producer = new FakeProducer(canned);
        var acc = new ProduceAccumulator("sync-batch");
        // holdMs 拉长到 3s：保证 5 条都进同一批（本用例手动触发发送，不等这 3s）
        acc.BatchMaxDelayMs(3000);

        List<Message> messages = MakeMessages(5);
        // 真实调用链里是 producer.CanBatch 先 TryAddMessage 记下字节额度，累加器在批次发完后
        // 再归还；这里直接调累加器，就得自己补上记账，否则 ReleaseHold 会把额度减成负数。
        foreach (Message m in messages)
        {
            Assert.True(acc.TryAddMessage(m));
        }

        var collected = new SendResult?[messages.Count];
        var workers = new List<Thread>();
        for (int i = 0; i < messages.Count; ++i)
        {
            int index = i;
            Message msg = messages[i];
            workers.Add(StartWorker(() => collected[index] = acc.Send(msg, producer)));
        }

        // 等 5 条都进批（它们此时都阻塞在 Add 里等阈值）
        Assert.True(WaitUntil(() => acc.SyncBatchCount == 1
                                    && acc.SyncBatchesSnapshot()[0].Count == 5, 3.0));
        MessageAccumulation theBatch = acc.SyncBatchesSnapshot()[0];
        Assert.Empty(theBatch.SendCallbacks);
        Assert.Empty(theBatch.Keys);

        // 手动触发（等价于守卫线程在 holdMs 到点后叫醒某一个等待者去发）
        lock (theBatch)
        {
            theBatch.Send();
        }

        foreach (Thread t in workers)
        {
            Assert.True(t.Join(TimeSpan.FromSeconds(5)));
        }

        Assert.Single(producer.Sent);
        (Message sentMsg, MessageQueue? sentMq, ISendCallback? sentCallback) = producer.Sent[0];
        Assert.IsType<MessageBatch>(sentMsg);
        Assert.Null(sentMq);        // 没指定 mq → 由 producer 轮询选
        Assert.Null(sentCallback);  // 同步路径没有回调
        Assert.Equal(ReferenceBatchBody(messages).Length, sentMsg.Body.Length);

        Assert.Equal(new[] { "id-0", "id-1", "id-2", "id-3", "id-4" },
            collected.Select(r => r!.MsgId).OrderBy(x => x, StringComparer.Ordinal).ToArray());
        foreach (SendResult? result in collected)
        {
            Assert.NotNull(result);
            int position = int.Parse(result!.MsgId.Substring(3), CultureInfo.InvariantCulture);
            Assert.Equal("off-" + position, result.OffsetMsgId);
            Assert.Equal(100 + position, result.QueueOffset);
        }

        // 发完归还全局额度
        Assert.Equal(0, acc.CurrentlyHoldSize);
    }

    [Fact]
    public void AsyncBatch_MergesAndFiresEveryCallback()
    {
        var canned = new SendResult
        {
            SendStatus = SendStatus.SendOk,
            MsgId = "a,b,c,d,e",
            MessageQueue = new MessageQueue(Topic, "broker-a", 0),
            QueueOffset = 7,
            OffsetMsgId = "p,q,r,s,t",
        };
        var producer = new FakeProducer(canned);
        var acc = new ProduceAccumulator("async-batch");
        acc.Start();
        var callbacks = new List<CollectingCallback>();
        for (int i = 0; i < 5; ++i)
        {
            callbacks.Add(new CollectingCallback());
        }

        List<Message> messages = MakeMessages(5);
        try
        {
            foreach (Message m in messages)
            {
                Assert.True(acc.TryAddMessage(m));
            }

            for (int i = 0; i < messages.Count; ++i)
            {
                acc.SendAsync(messages[i], callbacks[i], producer);
            }

            // 异步批次靠**守卫线程**唤醒：每 max(1, holdMs/2) ms 扫一遍，ReadyToSend 就发
            Assert.True(WaitUntil(() => callbacks.All(c => c.Total == 1), 5.0));
            Assert.True(WaitUntil(() => acc.CurrentlyHoldSize == 0, 2.0));
        }
        finally
        {
            acc.Shutdown();
        }

        Assert.All(callbacks, c => Assert.Empty(c.Errors));
        Assert.Equal(new[] { "a", "b", "c", "d", "e" },
            callbacks.Select(c => c.Results[0].MsgId).ToArray());
        Assert.Equal(new long[] { 7, 8, 9, 10, 11 },
            callbacks.Select(c => c.Results[0].QueueOffset).ToArray());

        Assert.Single(producer.Sent);
        (Message sentMsg, _, ISendCallback? sentCallback) = producer.Sent[0];
        var sentBatch = Assert.IsType<MessageBatch>(sentMsg);
        Assert.NotNull(sentCallback);   // 异步路径回调不为 null
        // 单线程依次 add → 批内顺序确定，可以逐字节比
        Assert.Equal(ReferenceBatchBody(messages), sentMsg.Body);
        Assert.Equal(string.Empty, sentBatch.Keys);
        Assert.True(sentBatch.Properties.ContainsKey(MessageConst.PropertyKeys));
    }

    [Fact]
    public void SplitResults_AreSharedWhenMsgIdHasNoComma()
    {
        // 老 broker / 单条应答：msgId 不含逗号时所有下标共享同一个 SendResult 对象。
        var shared = new SendResult
        {
            SendStatus = SendStatus.SendOk,
            MsgId = "single-id",
            MessageQueue = new MessageQueue(Topic, "b", 0),
            QueueOffset = 3,
        };
        var producer = new FakeProducer(shared);
        var acc = new ProduceAccumulator("shared-result");
        acc.BatchMaxDelayMs(3000);
        List<Message> messages = MakeMessages(3);
        var results = new SendResult?[messages.Count];
        var workers = new List<Thread>();
        for (int i = 0; i < messages.Count; ++i)
        {
            int index = i;
            Message msg = messages[i];
            workers.Add(StartWorker(() => results[index] = acc.Send(msg, producer)));
        }

        Assert.True(WaitUntil(() => acc.SyncBatchCount == 1
                                    && acc.SyncBatchesSnapshot()[0].Count == 3, 3.0));
        MessageAccumulation theBatch = acc.SyncBatchesSnapshot()[0];
        lock (theBatch)
        {
            theBatch.Send();
        }

        foreach (Thread t in workers)
        {
            t.Join(TimeSpan.FromSeconds(5));
        }

        Assert.All(results, r => Assert.Same(shared, r));
    }

    [Fact]
    public void SendWithMessageQueue_PinsTheBatch()
    {
        var mq = new MessageQueue(Topic, "broker-pinned", 2);
        var producer = new FakeProducer();
        var acc = new ProduceAccumulator("pinned");
        acc.BatchMaxDelayMs(3000);

        List<Message> messages = MakeMessages(2);
        var workers = messages.Select(m => StartWorker(() => acc.Send(m, mq, producer))).ToList();
        Assert.True(WaitUntil(() => acc.SyncBatchCount == 1
                                    && acc.SyncBatchesSnapshot()[0].Count == 2, 3.0));
        MessageAccumulation theBatch = acc.SyncBatchesSnapshot()[0];
        lock (theBatch)
        {
            theBatch.Send();
        }

        foreach (Thread t in workers)
        {
            t.Join(TimeSpan.FromSeconds(5));
        }

        Assert.Single(producer.Sent);
        Assert.Equal(mq, producer.Sent[0].Mq);

        // 异步 + 指定 mq
        var producer2 = new FakeProducer();
        var acc2 = new ProduceAccumulator("pinned-async");
        acc2.Start();
        var cb = new CollectingCallback();
        Message msg = MakeMessages(1)[0];
        try
        {
            Assert.True(acc2.TryAddMessage(msg));
            acc2.SendAsync(msg, mq, cb, producer2);
            Assert.True(WaitUntil(() => cb.Total == 1, 5.0));
        }
        finally
        {
            acc2.Shutdown();
        }

        Assert.Empty(cb.Errors);
        Assert.Single(producer2.Sent);
        Assert.Equal(mq, producer2.Sent[0].Mq);
    }

    [Fact]
    public void BatchMergesKeys_WithSpaceSeparator()
    {
        // 同步批次的 KEYS = 全体子消息 keys 的并集，空格 join（MessageConst.KeySeparator）。
        var producer = new FakeProducer();
        var acc = new ProduceAccumulator("keys");
        acc.BatchMaxDelayMs(3000);
        var m1 = new Message(Topic, Encoding.ASCII.GetBytes("aa")) { Keys = "k1 k2" };
        var m2 = new Message(Topic, Encoding.ASCII.GetBytes("bbb")) { Keys = "k2 k3" };
        // 参考批量体走 GenerateFromList 会逐条补 UNIQ_KEY，两边必须用同一批已打过 ID 的消息
        // （见文件头最后一段）。
        MessageClientIDSetter.SetUniqId(m1);
        MessageClientIDSetter.SetUniqId(m2);
        var workers = new List<Thread>
        {
            StartWorker(() => acc.Send(m1, producer)),
            StartWorker(() => acc.Send(m2, producer)),
        };
        Assert.True(WaitUntil(() => acc.SyncBatchCount == 1
                                    && acc.SyncBatchesSnapshot()[0].Count == 2, 3.0));
        MessageAccumulation theBatch = acc.SyncBatchesSnapshot()[0];
        lock (theBatch)
        {
            theBatch.Send();
        }

        foreach (Thread t in workers)
        {
            t.Join(TimeSpan.FromSeconds(5));
        }

        string keys = producer.Sent[0].Msg.Keys;
        Assert.Equal(new[] { "k1", "k2", "k3" }, keys.Split(' ').OrderBy(k => k).ToArray());
        // 同样只比长度（两条消息是两个线程并发 add 的，批内顺序不定）
        Assert.Equal(ReferenceBatchBody(new List<Message> { m1, m2 }).Length,
            producer.Sent[0].Msg.Body.Length);
    }

    // ---------------------------------------------------------------- 分区键

    [Fact]
    public void AggregateKey_PartitionsByTopicMqWaitAndTag()
    {
        var mq = new MessageQueue(Topic, "b", 0);
        var mine = new Message(Topic, Encoding.ASCII.GetBytes("x")) { Tags = "TagA" };
        var sameTag = new Message(Topic, Encoding.ASCII.GetBytes("y")) { Tags = "TagA" };
        var otherTag = new Message(Topic, Encoding.ASCII.GetBytes("y")) { Tags = "TagB" };
        var otherTopic = new Message("Other", Encoding.ASCII.GetBytes("y")) { Tags = "TagA" };
        Assert.Equal(AggregateKey.OfMessage(mine), AggregateKey.OfMessage(sameTag));
        Assert.NotEqual(AggregateKey.OfMessage(mine), AggregateKey.OfMessage(otherTag));
        Assert.NotEqual(AggregateKey.OfMessage(mine), AggregateKey.OfMessage(otherTopic));
        Assert.NotEqual(AggregateKey.OfMessage(mine), AggregateKey.OfMessageWithMq(mine, mq));
        Assert.Equal(AggregateKey.OfMessageWithMq(mine, mq),
            AggregateKey.OfMessageWithMq(sameTag, mq));
        // waitStoreMsgOK 参与分区
        var otherWait = new Message(Topic, Encoding.ASCII.GetBytes("y"))
        {
            Tags = "TagA",
            WaitStoreMsgOk = false,
        };
        Assert.NotEqual(AggregateKey.OfMessage(mine), AggregateKey.OfMessage(otherWait));
        // 缺 TAGS 属性时 tag 为 null（不是空串）—— Message.getTags() 的口径
        Assert.Null(AggregateKey.OfMessage(new Message(Topic, Encoding.ASCII.GetBytes("z"))).Tag);
    }

    [Fact]
    public void DifferentTags_DoNotMerge()
    {
        var producer = new FakeProducer();
        var acc = new ProduceAccumulator("tags");
        acc.BatchMaxDelayMs(3000);
        var m1 = new Message(Topic, Encoding.ASCII.GetBytes("aa")) { Tags = "TagA" };
        var m2 = new Message(Topic, Encoding.ASCII.GetBytes("bb")) { Tags = "TagB" };
        Thread t1 = StartWorker(() => acc.Send(m1, producer));
        Thread t2 = StartWorker(() => acc.Send(m2, producer));
        Assert.True(WaitUntil(() => acc.SyncBatchCount == 2
                                    && acc.SyncBatchesSnapshot().All(b => b.Count == 1), 3.0));
        foreach (MessageAccumulation batch in acc.SyncBatchesSnapshot())
        {
            lock (batch)
            {
                batch.Send();
            }
        }

        t1.Join(TimeSpan.FromSeconds(5));
        t2.Join(TimeSpan.FromSeconds(5));
        Assert.Equal(2, producer.Sent.Count);
        Assert.Equal(new[] { "TagA", "TagB" },
            producer.Sent.Select(s => s.Msg.Tags).OrderBy(t => t).ToArray());
    }

    // ---------------------------------------------------------------- 守卫线程

    [Fact]
    public void Guard_KeepsClosedBatchUntilNextSend()
    {
        var producer = new FakeProducer();
        var acc = new ProduceAccumulator("guard-keep");
        acc.Start();
        try
        {
            Message first = MakeMessages(1)[0];
            Assert.True(acc.TryAddMessage(first));
            acc.Send(first, producer);
            MessageAccumulation[] snapshot = acc.SyncBatchesSnapshot();
            Assert.Single(snapshot);
            Assert.True(snapshot[0].IsClosed);
            Assert.True(snapshot[0].MessagesSize > 0);
            // 全局额度已归还（finally 里 ReleaseHold(messagesSize)）
            Assert.Equal(0, acc.CurrentlyHoldSize);

            // 下一次同键发送：摘掉 closed 批次 → 新建 → 再发一批
            Message second = MakeMessages(1)[0];
            Assert.True(acc.TryAddMessage(second));
            acc.Send(second, producer);
            Assert.Equal(2, producer.Sent.Count);
            Assert.Equal(0, acc.CurrentlyHoldSize);
        }
        finally
        {
            acc.Shutdown();
        }
    }

    [Fact]
    public void Guard_RemovesEmptyBatchWithoutSending()
    {
        // 没人真的加消息的空批次：守卫线程直接置 closed + 摘表，不发任何东西。
        var producer = new FakeProducer();
        var acc = new ProduceAccumulator("guard-empty");
        AggregateKey key = AggregateKey.OfMessage(new Message(Topic, Encoding.ASCII.GetBytes("x")));
        MessageAccumulation empty = acc.PutEmptySyncBatch(key, producer);
        acc.RunGuardOnce(sync: true);
        Assert.True(empty.IsClosed);
        Assert.Equal(0, acc.SyncBatchCount);
        Assert.Empty(producer.Sent);
    }

    // ---------------------------------------------------------------- CanBatch / 转发层

    [Fact]
    public void CanBatch_RejectsDelayRetryAndPgroup()
    {
        var producer = new DefaultMQProducer("PG_CanBatch");
        var acc = new ProduceAccumulator("can-batch");
        acc.TotalBatchMaxBytes(1024 * 1024);
        producer.ProduceAccumulator = acc;

        Assert.True(producer.CanBatch(new Message(Topic, Encoding.ASCII.GetBytes("hello"))));

        var delayed = new Message(Topic, Encoding.ASCII.GetBytes("hello")) { DelayTimeLevel = 3 };
        Assert.False(producer.CanBatch(delayed));

        var timer = new Message(Topic, Encoding.ASCII.GetBytes("hello"));
        timer.PutProperty("TIMER_DELAY_MS", "100");
        Assert.False(producer.CanBatch(timer));

        var deliver = new Message(Topic, Encoding.ASCII.GetBytes("hello"));
        deliver.PutProperty("TIMER_DELIVER_MS", "100");
        Assert.False(producer.CanBatch(deliver));

        Assert.False(producer.CanBatch(new Message("%RETRY%GID_x", Encoding.ASCII.GetBytes("hello"))));

        var grouped = new Message(Topic, Encoding.ASCII.GetBytes("hello"));
        grouped.PutProperty("PGROUP", "PG_x");
        Assert.False(producer.CanBatch(grouped));

        Assert.True(acc.CurrentlyHoldSize > 0);
    }

    [Fact]
    public void SendByAccumulator_StampsUniqIdThenAccumulates()
    {
        var acc = new ProduceAccumulator("send-by-accum");
        acc.BatchMaxDelayMs(3000);
        var stub = new FakeProducer { ProduceAccumulator = acc };

        var msg = new Message(Topic, Encoding.ASCII.GetBytes("stamped"));
        Assert.Equal(string.Empty, MessageClientIDSetter.GetUniqId(msg));

        Thread worker = StartWorker(() => stub.SendByAccumulator(msg, null, null));
        Assert.True(WaitUntil(() => acc.SyncBatchCount == 1
                                    && acc.SyncBatchesSnapshot()[0].Count == 1, 3.0));
        // 攒批路径已经给这条消息打上 UNIQ_KEY（否则批量体的每条子消息都没有客户端 ID）
        Assert.NotEqual(string.Empty, MessageClientIDSetter.GetUniqId(msg));

        MessageAccumulation batch = acc.SyncBatchesSnapshot()[0];
        lock (batch)
        {
            batch.Send();
        }

        Assert.True(worker.Join(TimeSpan.FromSeconds(5)));
        Assert.Single(stub.Sent);
        Assert.IsType<MessageBatch>(stub.Sent[0].Msg);
    }

    [Fact]
    public void MessageBatch_NeverEntersAccumulator()
    {
        var producer = new DefaultMQProducer("PG_NoRecurse")
        {
            ClientId = "accum-norecurse-" + Guid.NewGuid().ToString("N"),
        };
        producer.InitProduceAccumulator();
        producer.AutoBatch = true;
        Assert.True(producer.AutoBatch);

        MessageBatch batch = MessageBatch.GenerateFromList(
            new List<Message> { new(Topic, Encoding.ASCII.GetBytes("a")) });
        // 没 Start 过 → 直发路径第一件事就是 GetClient()，抛"not started"；
        // 若它进了累加器，异常会变成累加器内部的行为（且不会是这个文案）。
        var ex = Assert.Throws<MQClientException>(() => producer.Send(batch));
        Assert.Contains("not started", ex.Message);
    }
}
