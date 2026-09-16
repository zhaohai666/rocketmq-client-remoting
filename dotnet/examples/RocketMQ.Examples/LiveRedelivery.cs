// 消费侧补齐真机验证（对齐 Java：回投 / 位点持久化 / 顺序锁 / 广播 / 流控 / rebalance / 注销）。
// 用法：rmq redelivery [namesrv]
//
// S1 回投 / S2 位点持久化 / S3 顺序消费+broker 锁 / S4 广播 / S5 流控
// S6 集群多实例 rebalance（均分队列、不重不漏、无重复消费）
// S7 优雅注销（shutdown 发 UNREGISTER_CLIENT，broker 端立刻摘除）
//
// ⚠ 每个场景都必须**先建 topic 再启动消费者**（见 PrepareTopic）。消费者不做默认 topic 兜底
//   （对齐 Java：只有生产者才会拿 TBW102 为新 topic 合成发布信息），topic 不存在时消费者拿不到
//   路由 → 不分配队列 → 不消费；等它自己发现路由时，CONSUME_FROM_LAST_OFFSET 已把位点解析到
//   "发现时刻的最新"，期间生产的消息会被正常跳过（Java 同样）。那是语义正确但测不出东西的
//   假失败，不是客户端 bug。
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
        ScenarioRebalance(producer);
        ScenarioUnregister();
        ScenarioNamespace();

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

    /// <summary>队列 key（与消费者内部 OffsetKey 一致）：topic + brokerName + queueId。</summary>
    private static string Key(MessageQueue mq) =>
        mq.Topic + mq.BrokerName + mq.QueueId.ToString(CultureInfo.InvariantCulture);

    /// <summary>按真实用法先把 topic 建出来，再启动消费者（与 Python/C++ 联调同义）。
    /// 真实环境里 topic 由管理员或首次发送预先创建；消费者不做默认 topic 兜底，
    /// topic 不存在时拿不到路由、不分配队列。</summary>
    private static void PrepareTopic(DefaultMQProducer producer, string topic, int queues = 4)
    {
        try
        {
            producer.CreateTopic("init", topic, queues);
        }
        catch (Exception e)
        {
            Console.WriteLine("  预建 topic " + topic + " 失败（改用自动创建）: " + e.Message);
        }

        // 等 NameServer 路由传播，否则消费者首轮 rebalance 仍查不到
        Thread.Sleep(3000);
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
        PrepareTopic(producer, topic);
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
        PrepareTopic(producer, topic);

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
        PrepareTopic(producer, topic);
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
        PrepareTopic(producer, topic);

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
        PrepareTopic(producer, topic);
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

    // ---------------- S6 集群多实例 rebalance（队列分配） ----------------
    private static void ScenarioRebalance(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_Rebalance";
        string group = _gPrefix + "_g6";
        PrepareTopic(producer, topic, 8);

        var la = new CollectingListenerConcurrently();
        var ca = NewConsumer(group);
        ca.InstanceName = "inst-a";
        ca.SetMessageListener(la);
        ca.Subscribe(topic, "*");
        ca.Start();

        var lb = new CollectingListenerConcurrently();
        var cb = NewConsumer(group);
        cb.InstanceName = "inst-b";
        cb.SetMessageListener(lb);
        cb.Subscribe(topic, "*");
        cb.Start();

        // 主 topic 的全部队列（期望被两实例完整覆盖）
        var expectedKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (MessageQueue mq in ca.FetchSubscribeMessageQueues(topic))
        {
            expectedKeys.Add(Key(mq));
        }

        // 等分配稳定：交集为空 + 两边都非空 + 主 topic 队列被完整覆盖（最多等 45s）
        List<string> keysA = new();
        List<string> keysB = new();
        long deadline = NowMs() + 45000;
        while (NowMs() < deadline)
        {
            keysA = ca.AssignedQueueKeys();
            keysB = cb.AssignedQueueKeys();
            var inter = new HashSet<string>(keysA, StringComparer.Ordinal);
            inter.IntersectWith(keysB);
            var covered = new HashSet<string>(keysA, StringComparer.Ordinal);
            covered.UnionWith(keysB);
            covered.IntersectWith(expectedKeys);
            if (keysA.Count > 0 && keysB.Count > 0 && inter.Count == 0
                && expectedKeys.Count > 0 && covered.Count == expectedKeys.Count)
            {
                break;
            }

            Thread.Sleep(2000);
        }

        long hbA = ca.HeartbeatCount;
        List<string> cidList = ca.ConsumerIdListOfGroup(topic);

        const int total = 40;
        for (int i = 0; i < total; ++i)
        {
            producer.Send(new Message(topic, Str2Bytes("rb-" + i.ToString(CultureInfo.InvariantCulture))));
        }

        Thread.Sleep(15000);

        List<string> gotA = la.Snapshot();
        List<string> gotB = lb.Snapshot();
        var all = new List<string>(gotA);
        all.AddRange(gotB);
        var uniq = new HashSet<string>(all, StringComparer.Ordinal);
        int dup = all.Count - uniq.Count;
        ca.Shutdown();
        cb.Shutdown();

        Check("S6-消费者已心跳注册", hbA > 0 && cidList.Count == 2,
            "heartbeats=" + hbA.ToString(CultureInfo.InvariantCulture)
                + " brokerCids=" + cidList.Count.ToString(CultureInfo.InvariantCulture));

        var sa = new HashSet<string>(keysA, StringComparer.Ordinal);
        var sb = new HashSet<string>(keysB, StringComparer.Ordinal);
        int cross = sa.Intersect(sb).Count();
        var cover = new HashSet<string>(sa, StringComparer.Ordinal);
        cover.UnionWith(sb);
        cover.IntersectWith(expectedKeys);
        Check("S6-队列不重不漏(a=" + keysA.Count.ToString(CultureInfo.InvariantCulture)
              + ",b=" + keysB.Count.ToString(CultureInfo.InvariantCulture)
              + ",交集=" + cross.ToString(CultureInfo.InvariantCulture)
              + ",覆盖=" + cover.Count.ToString(CultureInfo.InvariantCulture)
              + "/" + expectedKeys.Count.ToString(CultureInfo.InvariantCulture) + ")",
            keysA.Count > 0 && keysB.Count > 0 && cross == 0 && expectedKeys.Count > 0
                && cover.Count == expectedKeys.Count);
        Check("S6-消息无重复消费", dup == 0 && all.Count == total,
            "got=" + all.Count.ToString(CultureInfo.InvariantCulture)
                + "/" + total.ToString(CultureInfo.InvariantCulture)
                + " dup=" + dup.ToString(CultureInfo.InvariantCulture));
    }

    // ---------------- S7 优雅注销 ----------------
    private static void ScenarioUnregister()
    {
        // 复用 S6 建的 8 队列 topic；shutdown 时应发 UNREGISTER_CLIENT，broker 端立刻摘除，
        // 不必等心跳超时（~120s）。查询用独立的探针客户端（消费者 shutdown 后其内部客户端已关闭）。
        string topic = _gPrefix + "_Rebalance";
        string group = _gPrefix + "_g7";
        var probe = new MQClientInstance(
            "probe-" + NowMs().ToString(CultureInfo.InvariantCulture),
            new List<string> { _namesrv });
        probe.Start();

        var listener = new CollectingListenerConcurrently();
        var c = NewConsumer(group);
        c.InstanceName = "inst-c";
        c.SetMessageListener(listener);
        c.Subscribe(topic, "*");
        c.Start();
        Thread.Sleep(3000);

        string cid = c.ClientId;
        List<string> before = probe.GetConsumerIdListByGroup(topic, group) ?? new List<string>();
        c.Shutdown();
        Thread.Sleep(2000);
        List<string> after = probe.GetConsumerIdListByGroup(topic, group) ?? new List<string>();
        probe.Shutdown();

        Check("S7-shutdown 已注销 clientId",
            before.Contains(cid) && !after.Contains(cid),
            "before=" + before.Count.ToString(CultureInfo.InvariantCulture)
                + " after=" + after.Count.ToString(CultureInfo.InvariantCulture));
    }

    // ---------------- S8 命名空间（多租户隔离）----------------
    private static void ScenarioNamespace()
    {
        string ns = "NSDotnet" + (NowMs() % 100000).ToString(CultureInfo.InvariantCulture);
        string topic = _gPrefix + "_Ns";

        var nsProducer = new DefaultMQProducer(_gPrefix + "_ns_pg");
        nsProducer.NamesrvAddr = _namesrv;
        nsProducer.Namespace = ns;
        nsProducer.Start();
        // CreateTopic 也走 namespace 包装：真实建出来的是 "<ns>%<topic>"
        PrepareTopic(nsProducer, topic);

        var nsListener = new CollectingListenerConcurrently();
        var nsConsumer = new DefaultMQPushConsumer(_gPrefix + "_g8");
        nsConsumer.Namespace = ns;
        nsConsumer.SetNamesrvAddr(_namesrv);
        nsConsumer.SetMessageListener(nsListener);
        nsConsumer.Subscribe(topic, "*");
        nsConsumer.Start();
        Thread.Sleep(3000);
        for (int i = 0; i < 3; ++i)
        {
            nsProducer.Send(new Message(topic, Str2Bytes("ns-" + i.ToString(CultureInfo.InvariantCulture))));
        }

        Thread.Sleep(8000);
        nsConsumer.Shutdown();
        List<string> nsGot = nsListener.Snapshot();
        Check("S8-带 namespace 生产/消费收全", nsGot.Count == 3,
            "got=" + nsGot.Count.ToString(CultureInfo.InvariantCulture));

        // 不带 namespace 的消费者订阅同一个裸 topic → 收不到（证明真实 topic 是 ns%topic）
        var plainListener = new CollectingListenerConcurrently();
        try
        {
            var plainConsumer = new DefaultMQPushConsumer(_gPrefix + "_g8plain");
            plainConsumer.SetNamesrvAddr(_namesrv);
            plainConsumer.SetMessageListener(plainListener);
            plainConsumer.Subscribe(topic, "*");
            plainConsumer.Start();
            Thread.Sleep(8000);
            plainConsumer.Shutdown();
        }
        catch (Exception e)
        {
            Console.WriteLine("  (无 ns 消费者异常，符合隔离预期: " + e.Message + ")");
        }

        List<string> plainGot = plainListener.Snapshot();
        Check("S8-无 namespace 消费者收不到(隔离)", plainGot.Count == 0,
            "got=" + plainGot.Count.ToString(CultureInfo.InvariantCulture));

        nsProducer.Shutdown();
    }
}
