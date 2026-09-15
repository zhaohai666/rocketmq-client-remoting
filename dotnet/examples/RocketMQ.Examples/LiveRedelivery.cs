// 消费侧补齐真机验证（对齐 Java：回投 / 位点持久化 / 顺序锁 / 广播 / 流控）。
// 用法：rmq redelivery [namesrv]
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveRedelivery
{
    private static string _namesrv = "127.0.0.1:9876";
    private static string _gPrefix = string.Empty;

    private static int _pass;
    private static int _fail;

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok)
        {
            Interlocked.Increment(ref _pass);
            Console.WriteLine("  [PASS] " + name + (detail.Length > 0 ? "  " + detail : ""));
        }
        else
        {
            Interlocked.Increment(ref _fail);
            Console.WriteLine("  [FAIL] " + name + (detail.Length > 0 ? "  " + detail : ""));
        }
    }

    private static string Body(MessageExt m) => Encoding.UTF8.GetString(m.Body);

    private static byte[] Str2Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private sealed class CollectingListenerConcurrently : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<string> _bodies = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk) foreach (MessageExt m in msgs) _bodies.Add(Encoding.UTF8.GetString(m.Body));
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public List<string> Snapshot()
        {
            lock (_lk) return new List<string>(_bodies);
        }
    }

    public static int Run(string[] args)
    {
        _namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        _gPrefix = "GapDotnet_" + (NowMs() % 1000000).ToString(CultureInfo.InvariantCulture);
        Console.WriteLine("namesrv = " + _namesrv + "  prefix = " + _gPrefix);

        var producer = new DefaultMQProducer(_gPrefix + "_pg");
        producer.NamesrvAddr = _namesrv;
        producer.Start();
        Thread.Sleep(1000);

        ScenarioRetry(producer);
        ScenarioOffsetPersist(producer);
        ScenarioOrderlyLock(producer);
        ScenarioBroadcast(producer);
        ScenarioFlowControl(producer);

        producer.Shutdown();
        Console.WriteLine();
        Console.WriteLine("PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
            + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture));
        return _fail == 0 ? 0 : 1;
    }

    private static DefaultMQPushConsumer NewConsumer(string group)
    {
        var c = new DefaultMQPushConsumer(group);
        c.SetNamesrvAddr(_namesrv);
        return c;
    }

    // ---------------- S1 回投 ----------------
    private sealed class RetryListener : IMessageListenerConcurrently
    {
        public readonly object Lock = new();
        private readonly List<(string Body, string Topic, int ReconsumeTimes, long Ts)> Seen = new();

        public bool Orderly() => false;

        public List<(string Body, string Topic, int ReconsumeTimes, long Ts)> Snapshot()
        {
            lock (Lock) return new List<(string Body, string Topic, int ReconsumeTimes, long Ts)>(Seen);
        }

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            bool needRetry = false;
            lock (Lock)
            {
                foreach (MessageExt m in msgs)
                {
                    Seen.Add((Encoding.UTF8.GetString(m.Body), m.Topic, m.ReconsumeTimes, NowMs()));
                    if (Encoding.UTF8.GetString(m.Body) == "retry-me" && m.ReconsumeTimes == 0)
                    {
                        needRetry = true;
                    }
                }
            }

            // retry-me 首次投递失败，重投后成功（重投次数走 MessageExt 第 13 字段）
            return needRetry ? ConsumeConcurrentlyStatus.ReconsumeLater
                : ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    private static void ScenarioRetry(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_Retry";
        var consumer = NewConsumer(_gPrefix + "_g1");
        var listener = new RetryListener();
        consumer.SetMessageListener(listener);
        consumer.Subscribe(topic, "*");
        consumer.Start();
        Thread.Sleep(3000);
        producer.Send(new Message(topic, Str2Bytes("retry-me")));
        producer.Send(new Message(topic, Str2Bytes("normal-1")));
        Console.WriteLine("S1: 已发送，等待回投（延迟梯度 level3≈10s）...");
        Thread.Sleep(22000);
        consumer.Shutdown();

        List<(string Body, string Topic, int ReconsumeTimes, long Ts)> retryArrivals;
        int normalCount;
        var snap = listener.Snapshot();
        retryArrivals = snap.Where(a => a.Body == "retry-me").ToList();
        normalCount = snap.Count(a => a.Body == "normal-1");

        Check("S1-retry-me 被投递多次", retryArrivals.Count >= 2,
            "arrivals=" + retryArrivals.Count.ToString(CultureInfo.InvariantCulture));
        bool fromRetry = retryArrivals.Any(a => a.ReconsumeTimes >= 1 || a.Topic.StartsWith("%RETRY%", StringComparison.Ordinal));
        Check("S1-重投来自 %RETRY%/reconsumeTimes", fromRetry);
        if (retryArrivals.Count >= 2)
        {
            double gapSec = (retryArrivals[^1].Ts - retryArrivals[0].Ts) / 1000.0;
            Check("S1-回投有延迟梯度(>=8s)", gapSec >= 8.0,
                "gap=" + gapSec.ToString("F1", CultureInfo.InvariantCulture) + "s");
        }
        else
        {
            Check("S1-回投有延迟梯度(>=8s)", false, "不足两次投递");
        }

        Check("S1-正常消息只投一次", normalCount == 1,
            "arrivals=" + normalCount.ToString(CultureInfo.InvariantCulture));
    }

    // ---------------- S2 位点持久化 ----------------
    private static void ScenarioOffsetPersist(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_Offset";
        string group = _gPrefix + "_g2";

        List<string> round1;
        var c1 = NewConsumer(group);
        var l1 = new CollectingListenerConcurrently();
        c1.SetMessageListener(l1);
        c1.Subscribe(topic, "*");
        c1.Start();
        Thread.Sleep(3000);
        for (int i = 0; i < 3; ++i)
        {
            producer.Send(new Message(topic, Str2Bytes("persist-" + i.ToString(CultureInfo.InvariantCulture))));
        }

        Thread.Sleep(8000);
        c1.Shutdown();  // shutdown 持久化位点
        round1 = l1.Snapshot();

        int gotFirst = Enumerable.Range(0, 3).Count(i => round1.Contains("persist-" + i.ToString(CultureInfo.InvariantCulture)));
        Check("S2-首轮消费 3 条", gotFirst == 3, "got=" + gotFirst.ToString(CultureInfo.InvariantCulture));

        var c2 = NewConsumer(group);
        var l2 = new CollectingListenerConcurrently();
        c2.SetMessageListener(l2);
        c2.Subscribe(topic, "*");
        c2.Start();
        Thread.Sleep(3000);
        producer.Send(new Message(topic, Str2Bytes("persist-new")));
        Thread.Sleep(8000);
        c2.Shutdown();

        List<string> round2 = l2.Snapshot();
        bool newSeen = round2.Contains("persist-new");
        int oldResent = round2.Count(b => b.StartsWith("persist-", StringComparison.Ordinal) && b != "persist-new");
        Check("S2-重启后新消息继续投递", newSeen, newSeen ? "" : "未收到");
        Check("S2-重启不重复消费旧消息", oldResent == 0,
            "重复=" + oldResent.ToString(CultureInfo.InvariantCulture));
    }

    // ---------------- S3 顺序消费 + broker 锁 ----------------
    private sealed class CountingOrderlyListener : IMessageListenerOrderly
    {
        private int _got;

        public bool Orderly() => true;

        public ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext ctx)
        {
            Interlocked.Add(ref _got, msgs.Count);
            return ConsumeOrderlyStatus.Success;
        }

        public int Got => _got;
    }

    private static void ScenarioOrderlyLock(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_Orderly";
        var consumer = NewConsumer(_gPrefix + "_g3");
        var listener = new CountingOrderlyListener();
        consumer.SetMessageListener(listener);
        consumer.Subscribe(topic, "*");
        consumer.Start();
        Thread.Sleep(5000);  // 等 LOCK_BATCH_MQ 首轮生效
        for (int i = 0; i < 4; ++i)
        {
            producer.Send(new Message(topic, Str2Bytes("orderly-" + i.ToString(CultureInfo.InvariantCulture))));
        }

        Thread.Sleep(8000);
        consumer.Shutdown();
        Check("S3-顺序消费收全", listener.Got == 4,
            "got=" + listener.Got.ToString(CultureInfo.InvariantCulture));
        Check("S3-顺序消费链路存活", listener.Got > 0);
    }

    // ---------------- S4 广播模式 ----------------
    private static void ScenarioBroadcast(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_Bc";
        string group = _gPrefix + "_g4";

        var ca = NewConsumer(group);
        ca.InstanceName = "bc-a";
        var la = new CollectingListenerConcurrently();
        ca.SetMessageListener(la);
        ca.MessageModel = MessageModel.Broadcasting;
        ca.Subscribe(topic, "*");
        ca.Start();

        var cb = NewConsumer(group);
        cb.InstanceName = "bc-b";
        var lb = new CollectingListenerConcurrently();
        cb.SetMessageListener(lb);
        cb.MessageModel = MessageModel.Broadcasting;
        cb.Subscribe(topic, "*");
        cb.Start();

        Thread.Sleep(3000);
        for (int i = 0; i < 3; ++i)
        {
            producer.Send(new Message(topic, Str2Bytes("bc-" + i.ToString(CultureInfo.InvariantCulture))));
        }

        Thread.Sleep(8000);
        ca.Shutdown();
        cb.Shutdown();
        Check("S4-广播消费者 A 收全", la.Snapshot().Count == 3,
            "got=" + la.Snapshot().Count.ToString(CultureInfo.InvariantCulture));
        Check("S4-广播消费者 B 收全", lb.Snapshot().Count == 3,
            "got=" + lb.Snapshot().Count.ToString(CultureInfo.InvariantCulture));
    }

    // ---------------- S5 流控 ----------------
    private sealed class SlowListener : IMessageListenerConcurrently
    {
        private int _got;

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            Thread.Sleep(300);
            Interlocked.Add(ref _got, msgs.Count);
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public int Got => _got;
    }

    private static void ScenarioFlowControl(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_Flow";
        var consumer = NewConsumer(_gPrefix + "_g5");
        var listener = new SlowListener();
        consumer.SetMessageListener(listener);
        consumer.PullThresholdForQueue = 2;
        consumer.Subscribe(topic, "*");
        consumer.Start();
        Thread.Sleep(3000);
        for (int i = 0; i < 10; ++i)
        {
            producer.Send(new Message(topic, Str2Bytes("flow-" + i.ToString(CultureInfo.InvariantCulture))));
        }

        Thread.Sleep(12000);
        long fc = consumer.FlowControlTriggered;
        consumer.Shutdown();
        Check("S5-慢消费下消息全部到达", listener.Got == 10,
            "got=" + listener.Got.ToString(CultureInfo.InvariantCulture));
        Check("S5-流控触发计数>0", fc > 0,
            "triggered=" + fc.ToString(CultureInfo.InvariantCulture));
    }
}
