// 消费侧补齐真机验证（对齐 Java：回投 / 位点持久化 / 顺序锁 / 广播 / 流控 / rebalance / 注销）。
// 用法：rmq redelivery [namesrv]
//
// S1 回投 / S2 位点持久化 / S3 顺序消费+broker 锁 / S4 广播 / S5 流控
// S6 集群多实例 rebalance（均分队列、不重不漏、无重复消费，且两侧都收到 broker 反推的 40）
// S7 优雅注销（shutdown 发 UNREGISTER_CLIENT，broker 端立刻摘除）
// S9 死信终态：maxReconsumeTimes=2 ⇒ 恰好投递 3 次（reconsumeTimes 0/1/2），第 3 次回投后
//    broker 改投 %DLQ%<group>（自动建 topic 并注册路由），死信里 reconsumeTimes=3、
//    RETRY_TOPIC 保留业务 topic，且原组不再有第 4 次投递
// S10 部分 ack（ackIndex）：一批 3 条只认可第 1 条 ⇒ 尾巴 2 条经 %RETRY% 重投
//    （reconsumeTimes>=1、listener 看到业务 topic）、已认可的那条整个窗口只投一次、
//    3 条最终全部消费完、业务队列位点仍整批提交到 3；对照组（不碰 ackIndex）一条都不回投
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

    /// <summary>轮询到条件成立为止，返回是否在窗口内成立。
    /// 真机断言一律用它而不是固定 sleep：几套 live 并发跑时 broker 会拖慢，
    /// 固定 sleep 测出的是假失败（同一份代码复跑即绿）。</summary>
    private static bool WaitUntil(Func<bool> pred, int timeoutMs)
    {
        long deadline = NowMs() + timeoutMs;
        while (NowMs() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(1000);
        }

        return pred();
    }

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
        ScenarioDlq(producer);
        ScenarioPartialAck(producer);

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

        // 阈值 2 + 每条睡 300ms：全部落袋才说明「流控只是暂停拉取，不丢消息」。
        // 固定 sleep 在机器忙时会测出 got=7/10 的假失败（同一份代码复跑即绿）。
        bool allArrived = WaitUntil(() => listener.Got >= 10, 45000);
        long fc = consumer.FlowControlTriggered;
        consumer.Shutdown();
        Check("S5-慢消费下消息全部到达", allArrived && listener.Got == 10,
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
        // broker 在组成员变化时沿长连接反向推 40；反向请求用例注入不了，所以计数是
        // 唯一能证明「实例级 40 处理器真的跑过」的落点（必须在 Shutdown 之前取样）。
        long notifiedA = ca.Client().ConsumerIdsChangedCount;
        long notifiedB = cb.Client().ConsumerIdsChangedCount;
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
        Check("S6-成员变化时收到 broker 的 NOTIFY_CONSUMER_IDS_CHANGED(40)",
            notifiedA > 0 && notifiedB > 0,
            "a=" + notifiedA.ToString(CultureInfo.InvariantCulture)
                + " b=" + notifiedB.ToString(CultureInfo.InvariantCulture)
                + "（0 表示实例级处理器没收到过反向通知）");
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

    // ---------------- S9 死信终态（%DLQ%） ----------------
    private sealed class AlwaysFailListener : IMessageListenerConcurrently
    {
        public readonly object Lock = new();
        private readonly List<(string Body, string Topic, int ReconsumeTimes)> _seen = new();

        public bool Orderly() => false;

        public List<(string Body, string Topic, int ReconsumeTimes)> Snapshot()
        {
            lock (Lock) return new List<(string Body, string Topic, int ReconsumeTimes)>(_seen);
        }

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            bool mine = false;
            lock (Lock)
            {
                foreach (MessageExt m in msgs)
                {
                    string body = Body(m);
                    _seen.Add((body, m.Topic, m.ReconsumeTimes));
                    if (body == "dlq-me") mine = true;
                }
            }

            return mine ? ConsumeConcurrentlyStatus.ReconsumeLater
                : ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    /// <summary>
    /// 死信终态。为什么只能真机验：「重试到第几次算用尽」两端各写一半——客户端只把
    /// maxReconsumeTimes 塞进 CONSUMER_SEND_MSG_BACK 请求头（Java
    /// DefaultMQPushConsumerImpl#sendMessageBack:773，-1 时按 16 传，见 #getMaxReconsumeTimes:890），
    /// 判定与改投 %DLQ%&lt;group&gt; 全在 broker（AbstractSendMessageProcessor#consumerSendMsgBack:183
    /// 用 `msgExt.getReconsumeTimes() &gt;= maxReconsumeTimes`，注意是 &gt;= 不是 &gt;；转死信时
    /// topic 换成 MixAll.GetDlqTopic(group)、顺手建 topic 并注册路由，:226 又给 reconsumeTimes +1）。
    /// 两种写反都表现为「看起来正常」：漏传 header ⇒ broker 用订阅组默认 16 次，测试等到天荒地老；
    /// &gt;= 写成 &gt; ⇒ 多投一次才进死信。离线单测锁不住任何一边。
    /// </summary>
    private static void ScenarioDlq(DefaultMQProducer producer)
    {
        const int maxReconsume = 2;
        string topic = _gPrefix + "_Dlq";
        string group = _gPrefix + "_g9";
        PrepareTopic(producer, topic, 1);

        var consumer = NewConsumer(group);
        consumer.MaxReconsumeTimes = maxReconsume;
        var listener = new AlwaysFailListener();
        consumer.SetMessageListener(listener);
        consumer.Subscribe(topic, "*");
        consumer.Start();
        Thread.Sleep(3000);
        producer.Send(new Message(topic, Str2Bytes("dlq-me")));

        // 回投档位 = 3 + reconsumeTimes ⇒ level3(10s) + level4(30s)，再留投递余量。
        // 150s 而不是 100s：整机并发跑其它套件时 broker 的定时服务会拖档，第三次投递
        // 实测能晚到 60s+，卡 100s 是假失败。
        long deadline = NowMs() + 150000;
        while (NowMs() < deadline && listener.Snapshot().Count < 3) Thread.Sleep(2000);
        List<(string Body, string Topic, int ReconsumeTimes)> firstThree =
            listener.Snapshot().Where(a => a.Body == "dlq-me").ToList();
        Thread.Sleep(15000);  // 反证：不该有第 4 次投递
        List<(string Body, string Topic, int ReconsumeTimes)> finalSeen =
            listener.Snapshot().Where(a => a.Body == "dlq-me").ToList();
        consumer.Shutdown();

        string times = string.Join(",", firstThree.Select(a => a.ReconsumeTimes
            .ToString(CultureInfo.InvariantCulture)));
        Check("S9-maxReconsumeTimes=2 ⇒ 投递 3 次（reconsumeTimes 0/1/2）",
            firstThree.Count >= 3 && firstThree[0].ReconsumeTimes == 0
                && firstThree[1].ReconsumeTimes == 1 && firstThree[2].ReconsumeTimes == 2,
            "times=[" + times + "]");
        Check("S9-用尽后不再投递（观察窗口内只有 3 次）", finalSeen.Count == 3,
            "arrivals=" + finalSeen.Count.ToString(CultureInfo.InvariantCulture));
        Check("S9-重投期间 listener 看到业务 topic（不是 %RETRY%）",
            firstThree.All(a => a.Topic == topic));

        // %DLQ%<group> 由 broker 在转死信那一刻才建出来并注册到 namesrv
        string dlqTopic = MixAll.GetDlqTopic(group);
        TopicRouteData? route = null;
        using (var probe = new MQClientInstance("dlqprobe-" + NowMs().ToString(CultureInfo.InvariantCulture),
                   new List<string> { _namesrv }))
        {
            probe.Start();
            for (int i = 0; i < 15; ++i)
            {
                route = probe.GetTopicRouteData(dlqTopic);
                if (route != null && route.QueueDatas.Count > 0) break;
                route = null;
                Thread.Sleep(2000);
            }
        }

        Check("S9-broker 自动创建并注册了 %DLQ%<group> 路由", route != null,
            "dlq=" + dlqTopic);

        var dlqMsgs = new List<MessageExt>();
        if (route != null)
        {
            var queues = new List<MessageQueue>();
            foreach (QueueData q in route.QueueDatas)
            {
                for (int i = 0; i < q.ReadQueueNums; ++i)
                {
                    queues.Add(new MessageQueue(dlqTopic, q.BrokerName, i));
                }
            }

            var reader = new DefaultLitePullConsumer(_gPrefix + "_g9dlq");
            reader.SetNamesrvAddr(_namesrv);
            // 新消费组 + LAST 会从队尾开始，把已经在死信里的那条跳过 ⇒ 假失败
            reader.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
            reader.Assign(queues);
            reader.Start();
            foreach (MessageQueue mq in queues) reader.SeekToBegin(mq);
            deadline = NowMs() + 25000;
            while (NowMs() < deadline && dlqMsgs.Count == 0) dlqMsgs.AddRange(reader.Poll(1000));
            reader.Shutdown();
        }

        bool one = dlqMsgs.Count == 1 && Body(dlqMsgs[0]) == "dlq-me";
        Check("S9-消息落在 %DLQ%<group>", one,
            "n=" + dlqMsgs.Count.ToString(CultureInfo.InvariantCulture));
        if (one)
        {
            MessageExt d = dlqMsgs[0];
            Check("S9-死信 reconsumeTimes = maxReconsumeTimes + 1（broker 存储时 +1）",
                d.ReconsumeTimes == maxReconsume + 1,
                "reconsumeTimes=" + d.ReconsumeTimes.ToString(CultureInfo.InvariantCulture));
            bool retryKept = d.Properties.TryGetValue("RETRY_TOPIC", out string? origin)
                && origin == topic && d.Topic == dlqTopic;
            Check("S9-死信保留 RETRY_TOPIC=业务 topic，topic 已是 %DLQ%<group>", retryKept,
                "retryTopic=" + (d.Properties.TryGetValue("RETRY_TOPIC", out string? o)
                    ? o : "<missing>"));
        }
    }

    // ---------------- S10 部分 ack（ackIndex） ----------------
    /// <summary>记录每条投递，并（可选）只在**首批**把 ctx.AckIndex 收窄。</summary>
    private sealed class PartialAckListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<(string Body, string Topic, int ReconsumeTimes)> _seen = new();
        private readonly List<int> _batchSizes = new();

        /// <summary>&lt; 0 = 完全不碰 ackIndex（对照组，走 Java 默认的整批认可）。</summary>
        private readonly int _ackFirst;

        public PartialAckListener(int ackFirst) => _ackFirst = ackFirst;

        public bool Orderly() => false;

        public List<(string Body, string Topic, int ReconsumeTimes)> Snapshot()
        {
            lock (_lk)
            {
                return new List<(string Body, string Topic, int ReconsumeTimes)>(_seen);
            }
        }

        public int BatchCount => _batchSizes.Count;

        public int FirstBatchSize => _batchSizes.Count > 0 ? _batchSizes[0] : 0;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            bool narrow = false;
            lock (_lk)
            {
                foreach (MessageExt m in msgs) _seen.Add((Body(m), m.Topic, m.ReconsumeTimes));
                _batchSizes.Add(msgs.Count);
                // 只收窄首批：后续批次必须整批认可，否则那条尾巴永远回投不完，
                // 「已认可前缀只投一次」的计数器也会被后续批次污染。
                narrow = _ackFirst >= 0 && _batchSizes.Count == 1;
            }

            if (narrow) ctx.AckIndex = _ackFirst;
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    /// <summary>
    /// 部分 ack（ackIndex）。为什么只能真机验：Java
    /// ConsumeMessageConcurrentlyService#processConsumeResult:207-269 在 CONSUME_SUCCESS 时
    /// 把 listener 写的 ackIndex 当切点——前缀提交位点、尾巴逐条 sendMessageBack；
    /// 默认 Integer.MAX_VALUE 就是整批认可。离线单测（tests/.../ConsumeAckIndexTests.cs）
    /// 只能锁「回投**失败**时位点不越过它」——未 start 的消费者回投必败；
    /// 「回投成功时尾巴真被 broker 收下重投、已 ack 的那条整个窗口只投一次、业务队列位点仍整批
    /// 前进」只有真 broker 说了算。两种写错在离线看不出差别：忘记回投（尾巴静默丢失，
    /// 收到的条数照样对）、把已 ack 的前缀也回投（看起来"没丢"，其实是重复投递）。
    /// </summary>
    private static void ScenarioPartialAck(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_AckIndex";
        // 1 队列：一批 3 条才连续且有序，尾巴才是真尾巴
        PrepareTopic(producer, topic, 1);
        string group = _gPrefix + "_g10";
        string controlGroup = _gPrefix + "_g10ctrl";

        var partial = new PartialAckListener(0);
        var control = new PartialAckListener(-1);
        var pc = NewConsumer(group);
        var cc = NewConsumer(controlGroup);
        // 默认一批 1 条，不收窄到 3 根本没有「部分」可言
        pc.ConsumeMessageBatchMaxSize = 3;
        cc.ConsumeMessageBatchMaxSize = 3;
        pc.SetMessageListener(partial);
        cc.SetMessageListener(control);
        pc.Subscribe(topic, "*");
        cc.Subscribe(topic, "*");
        // 先把 3 条放上去再起消费者：批次怎么切由拉取时机决定，队列里已经躺着 3 条时第一次
        // 拉取才会正好是「一整批 3 条」，否则首批可能只有 1~2 条，ackIndex=0 划出的
        // 前缀/后缀就不确定了。新组在 LAST_OFFSET 下会从分配时刻的最新位点开始，故显式从 0 起消。
        pc.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
        cc.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
        for (int i = 0; i < 3; ++i)
        {
            producer.Send(new Message(topic, Str2Bytes("ack-" + i.ToString(CultureInfo.InvariantCulture))));
        }
        pc.Start();
        cc.Start();

        WaitUntil(() => partial.Snapshot().Count >= 3 && control.Snapshot().Count >= 3, 40000);

        string[] tail = { "ack-1", "ack-2" };

        int Redelivered(PartialAckListener l)
        {
            List<(string Body, string Topic, int ReconsumeTimes)> seen = l.Snapshot();
            int hit = 0;
            foreach (string b in tail)
            {
                if (seen.Any(r => r.Body == b && r.ReconsumeTimes >= 1 && r.Topic == topic)) ++hit;
            }

            return hit;
        }

        // 尾巴要经 %RETRY%（延迟 level 3≈10s）+ 重试 topic 的路由注册 + 下一轮 rebalance
        bool tailBack = WaitUntil(() => Redelivered(partial) == tail.Length, 150000);
        Thread.Sleep(10000);  // 反证窗口：多余的重复投递会露出来
        List<(string Body, string Topic, int ReconsumeTimes)> seenFinal = partial.Snapshot();

        Check("S10-首批确实拿到 3 条（ackIndex=0 才有「部分」可言）",
            partial.FirstBatchSize == 3,
            "firstBatch=" + partial.FirstBatchSize.ToString(CultureInfo.InvariantCulture));
        Check("S10-未认可的尾巴从 %RETRY% 回来（reconsumeTimes>=1 且 topic 是业务 topic）",
            tailBack,
            "redelivered=" + Redelivered(partial).ToString(CultureInfo.InvariantCulture) + "/2");
        int ackedHits = seenFinal.Count(r => r.Body == "ack-0");
        Check("S10-已认可的那条整个窗口只投一次（没有把前缀也回投）", ackedHits == 1,
            "arrivals=" + ackedHits.ToString(CultureInfo.InvariantCulture));
        Check("S10-3 条最终全部消费（不丢）",
            seenFinal.Select(r => r.Body).Distinct().Count() == 3,
            "distinct=" + seenFinal.Select(r => r.Body).Distinct().Count()
                .ToString(CultureInfo.InvariantCulture));

        List<(string Body, string Topic, int ReconsumeTimes)> ctrl = control.Snapshot();
        Check("S10-对照组默认 ackIndex(MAX_VALUE)：一条都不回投",
            ctrl.Count == 3 && ctrl.All(r => r.ReconsumeTimes == 0),
            "deliveries=" + ctrl.Count.ToString(CultureInfo.InvariantCulture)
            + " retried=" + ctrl.Count(r => r.ReconsumeTimes >= 1).ToString(CultureInfo.InvariantCulture));

        // broker 侧口径：两个组的业务队列位点都必须整批提交到 3（部分 ack 不是「少提交」，
        // 尾巴已交给 broker 重投，本队列没有欠账）
        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_namesrv);
        admin.SetTimeoutMillis(10000);
        try
        {
            admin.Start();
            List<MessageQueue> queues = pc.FetchSubscribeMessageQueues(topic);
            Check("S10-业务 topic 有队列可查位点", queues.Count > 0,
                "queues=" + queues.Count.ToString(CultureInfo.InvariantCulture));
            if (queues.Count > 0)
            {
                MessageQueue mq = queues[0];
                // -1 = broker 还没有该组的位点（QUERY_NOT_FOUND）
                long ReadOffset(string g)
                {
                    try
                    {
                        return admin.ExamineConsumerOffset(g, mq, out long off) ? off : -1;
                    }
                    catch (Exception)
                    {
                        return -1;
                    }
                }

                bool p = WaitUntil(() => ReadOffset(group) == 3, 30000);
                Check("S10-部分 ack 后业务队列位点仍整批前进到 3", p,
                    "committed=" + ReadOffset(group).ToString(CultureInfo.InvariantCulture));
                bool c = WaitUntil(() => ReadOffset(controlGroup) == 3, 30000);
                Check("S10-对照组业务队列位点同样到 3", c,
                    "committed=" + ReadOffset(controlGroup).ToString(CultureInfo.InvariantCulture));
            }
        }
        catch (Exception e)
        {
            Check("S10-位点查询可用", false, e.Message);
        }

        admin.Shutdown();
        pc.Shutdown();
        cc.Shutdown();
    }
}
