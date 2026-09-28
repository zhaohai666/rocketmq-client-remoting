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
// S11 拉取停摆自愈（Java isPullExpired，阈值 120s）：1 队列 topic 先消费 3 条并把位点提交到 3，
//    再把该队列登记的循环线程换成一条永远起不来的占位线程（= 循环被异常打穿）⇒ 立刻判停摆 ⇒
//    叫醒生产 rebalance ⇒ 断言它被撤掉重建（新线程接管并重新盖章），之后 3 条照样消费、
//    位点前进到 6；另有一轮"盖章倒拨 125s"验证阈值那一支与运行信息的 lastPullTimestamp；
//    最终 9 条各只投一次、reconsumeTimes 全 0（撤走前持久化了位点，重建后从 broker 续拉）
// S12 顺序消费毒消息：listener 一直 SUSPEND + maxReconsumeTimes=2 ⇒ 本地恰好投 3 次
//    （reconsumeTimes 0/1/2，每次自己 +1），第 3 次交 broker 后业务队列继续前进，消息因
//    rebalance 锁未过期被 broker 立刻改投 %DLQ%<group>（reconsumeTimes=3、RETRY_TOPIC 保留业务 topic）
// S12b 顺序侧的 -1 是**不设上限**（投过 >=18 次、%DLQ% 空），不是并发侧的 16
// S12c context.SuspendCurrentQueueTimeMillis 优先于消费者配置（配置 900/context 70 ⇒ 相邻投递
//    中位间隔贴着 70ms+50ms 节拍；context -1 ⇒ 回落配置 400ms；1ms/0 两个非法值钳到下限不忙等）
// S13 顺序侧显式批量 ack（COMMIT，autoCommit=false）/ 显式回滚（ROLLBACK 当场重投）：
//    Java ConsumeMessageOrderlyService#processConsumeResult:270-300 —— a) COMMIT 一批 3 条一次
//    认可（位点直接到 3）；b) ROLLBACK 把这条退回**本地**重投（间隔贴着 200ms 挂起，reconsumeTimes
//    不动、topic 不变、后面的消息不越位；回滚 6 次后提交，位点到 2）；c) autoCommit=true 时
//    ROLLBACK 是非法用法，只 warn 并按成功 ack（head 只投一次、next 立刻被消费、位点到 2）
// S14 correctTagsOffset（Java DefaultMQPushConsumerImpl:713-717，调用点 :394-401）：空应答
//    （NO_NEW_MSG / NO_MATCHED_MSG）时把"已消费位点"抬到拉取游标（只前进不回退，且闸门要求
//    ProcessQueue 里既没有待消费也没有在途批次）。三腿：a) 对照组 TagA 正常收齐 5 条且
//    committed == 各队列 maxOffset（数值口径）；b) TagB 永不匹配 ⇒ listener 零投递但每条队列的
//    committed 仍 == maxOffset（不修正就永远查无记录）；c) %RETRY%<group> 空队列出现 0 位点记录
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
        ScenarioPullStallSelfHeal(producer);
        ScenarioOrderlyDlq(producer);
        ScenarioOrderlyNoCap(producer);
        ScenarioOrderlySuspendMillis(producer);
        ScenarioOrderlyAckRollback(producer);
        ScenarioCorrectTagsOffset(producer);

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
        (bool routeFound, List<MessageExt> dlqMsgs) = ReadDlq(group, _gPrefix + "_g9dlq", 30000);
        Check("S9-broker 自动创建并注册了 %DLQ%<group> 路由", routeFound, "dlq=" + dlqTopic);

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

    // ---------------- S11 拉取停摆自愈（Java isPullExpired / PULL_MAX_IDLE_TIME=120s）----------------

    /// <summary>
    /// 一路拉取循环死了之后，下一趟 rebalance 必须把它**撤掉重建**，而且撤走前要把已消费位点
    /// 持久化（Java RebalanceImpl#updateProcessQueueTableInRebalance:438-461 的 [BUG] 分支 +
    /// removeUnnecessaryMessageQueue）。
    /// </summary>
    /// <remarks>
    /// 为什么必须真机：判据本身（120s 阈值、严格 <c>&gt;</c>、撤走清哪些痕迹、撤/建顺序）
    /// 已经由 tests/RocketMQ.Client.Tests/PullExpiredTests.cs 离线锁死，但"重建出来的那一路
    /// 真的从 broker 位点接着往下消费、一条不重不丢"只有真 broker 能证明 —— 少持久化那一步
    /// 在离线测试里看着照样能消费，真机才会暴露成"整把队列从旧位点重投"。
    ///
    /// 注入方式与 C++/Rust 那两版不同，是有意的：.NET 的存活判据就是登记线程本身
    /// （<c>Thread.IsAlive</c>，Java per-queue ProcessQueue 的等价物），所以这里把登记表里那一路
    /// 换成一条**永远起不来**的占位线程。好处是判据没有时钟竞态 —— 自愈之前
    /// <c>PullStalled</c> 恒为 true（占位线程不可能变 alive），只有被换成真的新循环才会 false；
    /// 用"倒拨盖章"注入反而会撞上旧循环自己刷新的那一次，判不出到底自愈没有。
    /// 阈值那一支（H3）仍然照拨，但只断言"拨了会判停摆、运行信息如实报出、之后照常消费"。
    /// </remarks>
    private static void ScenarioPullStallSelfHeal(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_Heal";
        string group = _gPrefix + "_g11";
        // 1 队列：只有一路循环，注入点唯一，位点判据也唯一（多队列会被分摊，撤走的不一定是注入那把）
        PrepareTopic(producer, topic, 1);

        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_namesrv);
        admin.SetTimeoutMillis(10000);
        admin.Start();

        var listener = new RetryListener();
        var c = NewConsumer(group);
        c.SetMessageListener(listener);
        c.Subscribe(topic, "*");
        c.Start();

        try
        {
            List<MessageQueue> queues = new();
            WaitUntil(() =>
            {
                queues = c.FetchSubscribeMessageQueues(topic);
                return queues.Count == 1 && c.AssignedQueueKeys().Contains(Key(queues[0]));
            }, 30000);
            Check("S11-单队列已分到本实例", queues.Count == 1 && c.AssignedQueueKeys().Contains(Key(queues[0])),
                "queues=" + queues.Count.ToString(CultureInfo.InvariantCulture)
                + " assigned=" + string.Join(",", c.AssignedQueueKeys()));
            if (queues.Count != 1)
            {
                return;
            }

            MessageQueue mq = queues[0];
            string key = Key(mq);

            long Committed()
            {
                try
                {
                    return admin.ExamineConsumerOffset(group, mq, out long off) ? off : -1;
                }
                catch (Exception)
                {
                    return -1;
                }
            }

            // ---- H1 基线：3 条消费掉，位点提交到 3，拉取时钟是真值 ----
            for (int i = 0; i < 3; ++i)
            {
                producer.Send(new Message(topic, Str2Bytes("heal-1-" + i.ToString(CultureInfo.InvariantCulture))));
            }

            Check("S11-H1 基线：3 条被消费",
                WaitUntil(() => listener.Snapshot().Count >= 3, 30000),
                "arrivals=" + listener.Snapshot().Count.ToString(CultureInfo.InvariantCulture));
            Check("S11-H1 基线：位点提交到 broker（撤走重建全靠它续拉）",
                WaitUntil(() => Committed() == 3, 30000),
                "committed=" + Committed().ToString(CultureInfo.InvariantCulture));

            // 运行信息里的 lastPullTimestamp 必须是真时刻（Java ProcessQueue#fillOutRunningInfo:456）
            long stamp = c.LastPullAt(key);
            Check("S11-H1 循环在发起拉取时盖章", stamp > 0 && NowMs() - stamp < 60000,
                "lastPullAt=" + stamp.ToString(CultureInfo.InvariantCulture));
            Check("S11-H1 进程内已消费位点是 3（自愈时要带着它去持久化）",
                c.ConsumeOffsetForTest(key) == 3,
                "consumeOffset=" + (c.ConsumeOffsetForTest(key) ?? -1).ToString(CultureInfo.InvariantCulture));

            // ---- H2 循环被异常打穿 → 同一趟 rebalance 撤掉重建 ----
            c.RegisterLoopForTest(key, alive: false);
            Check("S11-H2 线程已退出即刻判停摆（不必等满 120s）", c.PullStalled(key), "key=" + key);
            c.WakeupRebalanceForTest();
            // 只有"登记条目被换成一条真活着的线程"才会让 PullStalled 变 false：
            // 注入的占位线程永远不会 alive，所以这一条断言就等价于"确实撤掉并重建了"。
            Check("S11-H2 停摆队列被撤掉重建（新循环接管并重新盖章）",
                WaitUntil(() => !c.PullStalled(key), 30000),
                "lastPullAt=" + c.LastPullAt(key).ToString(CultureInfo.InvariantCulture));
            Check("S11-H2 重建没有把位点写回去退", Committed() >= 3,
                "committed=" + Committed().ToString(CultureInfo.InvariantCulture));

            for (int i = 0; i < 3; ++i)
            {
                producer.Send(new Message(topic, Str2Bytes("heal-2-" + i.ToString(CultureInfo.InvariantCulture))));
            }

            Check("S11-H2 自愈后同一个队列继续消费（累计 6 条）",
                WaitUntil(() => listener.Snapshot().Count >= 6, 30000) && WaitUntil(() => Committed() == 6, 30000),
                "arrivals=" + listener.Snapshot().Count.ToString(CultureInfo.InvariantCulture)
                + " committed=" + Committed().ToString(CultureInfo.InvariantCulture));

            // ---- H3 盖章超出 120s（线程还活着）----
            long injected = NowMs() - DefaultMQPushConsumer.PullMaxIdleTimeMillis - 5000;
            string want = "\"lastPullTimestamp\":" + injected.ToString(CultureInfo.InvariantCulture);
            bool reported = false;
            bool expired = false;
            // 重拨而不是只拨一次：真循环每发起一轮就会自己刷新这个时刻，撞上一次的概率很低
            // 但不为零（长轮询刚好在这几毫秒里返回），重拨一次就能同时观察到判据和报文。
            for (int attempt = 0; attempt < 5 && !(reported && expired); ++attempt)
            {
                c.SetLastPullAt(key, injected);
                expired = c.PullStalled(key);
                reported = Encoding.UTF8.GetString(c.BuildConsumerRunningInfo().Encode()).Contains(want);
            }

            Check("S11-H3 倒拨超过 120s 即判停摆", expired, "injected=" + injected);
            Check("S11-H3 运行信息把 lastPullTimestamp 报成盖章的那个值", reported, "want=" + want);
            c.WakeupRebalanceForTest();
            Check("S11-H3 停摆分支被处理（这一路重新盖章或换上了新循环）",
                WaitUntil(() => c.LastPullAt(key) > injected, 30000),
                "lastPullAt=" + c.LastPullAt(key).ToString(CultureInfo.InvariantCulture));

            for (int i = 0; i < 3; ++i)
            {
                producer.Send(new Message(topic, Str2Bytes("heal-3-" + i.ToString(CultureInfo.InvariantCulture))));
            }

            Check("S11-H3 之后仍然照常消费（累计 9 条、位点到 9）",
                WaitUntil(() => listener.Snapshot().Count >= 9, 30000) && WaitUntil(() => Committed() == 9, 30000),
                "arrivals=" + listener.Snapshot().Count.ToString(CultureInfo.InvariantCulture)
                + " committed=" + Committed().ToString(CultureInfo.InvariantCulture));

            // 留一个窗口给"重建时拿陈旧游标回退位点"这类错误显形：整把队列重投会在这里露出来
            Thread.Sleep(8000);
            List<(string Body, string Topic, int ReconsumeTimes, long Ts)> seen = listener.Snapshot();
            int distinct = seen.Select(r => r.Body).Distinct().Count();
            int redelivered = seen.Count(r => r.ReconsumeTimes != 0);
            Check("S11-9 条各只投一次（撤走前持久化了位点，重建后从 broker 位点续拉）",
                distinct == 9 && redelivered == 0 && seen.Count == 9,
                "arrivals=" + seen.Count.ToString(CultureInfo.InvariantCulture)
                + " distinct=" + distinct.ToString(CultureInfo.InvariantCulture)
                + " redelivered=" + redelivered.ToString(CultureInfo.InvariantCulture));
            Check("S11-自愈之后拉取时钟恢复新鲜（判据不再报警）",
                !c.PullStalled(key) && NowMs() - c.LastPullAt(key) < 60000,
                "age=" + (NowMs() - c.LastPullAt(key)).ToString(CultureInfo.InvariantCulture));
        }
        finally
        {
            c.ReleaseTestLoops();
            c.Shutdown();
            admin.Shutdown();
        }
    }

    // ---------------- S12 顺序消费毒消息：本地计数 → 交 broker → %DLQ% ----------------

    /// <summary>命中指定 body 的每次顺序投递都挂起当前队列（其余照常成功）。</summary>
    private sealed class PoisonOrderlyListener : IMessageListenerOrderly
    {
        private readonly string _poison;
        private readonly object _gate = new();
        private readonly List<(string Body, string Topic, int ReconsumeTimes)> _seen = new();

        public PoisonOrderlyListener(string poison)
        {
            _poison = poison;
        }

        public bool Orderly() => true;

        public List<(string Body, string Topic, int ReconsumeTimes)> Snapshot()
        {
            lock (_gate) return new List<(string Body, string Topic, int ReconsumeTimes)>(_seen);
        }

        public int CountOf(string body)
        {
            lock (_gate) return _seen.Count(a => a.Body == body);
        }

        public ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext ctx)
        {
            bool mine = false;
            lock (_gate)
            {
                foreach (MessageExt m in msgs)
                {
                    string body = Body(m);
                    _seen.Add((body, m.Topic, m.ReconsumeTimes));
                    if (body == _poison) mine = true;
                }
            }

            return mine ? ConsumeOrderlyStatus.SuspendCurrentQueueAMoment
                : ConsumeOrderlyStatus.Success;
        }
    }

    /// <summary>
    /// 顺序消费的毒消息终态。为什么只能真机验：
    /// Java ConsumeMessageOrderlyService#processConsumeResult:236-307 的 SUSPEND 分支先过
    /// checkReconsumeTimes:322-339 —— 次数没用尽就**本地** reconsumeTimes +1 并原地挂起
    /// （broker 那边压根没记这次失败），用尽了才 sendMessageBack:341-362 把整条投给
    /// %RETRY%&lt;group&gt;，**投成功就不再挂起**、commit 位点让路（所以下一条必须被消费）。
    /// 而「投给 %RETRY% 之后进不进 %DLQ%」全在 broker：SendMessageProcessor#handleRetryAndDLQ:185-234
    /// 读 SEND_MESSAGE_V2 的 j/l（AbstractSendMessageProcessor:427 把 j 直接写成存储消息的
    /// reconsumeTimes），且只有本组 rebalance 锁还没过期（:202-207，即这个实例真的握着
    /// LOCK_BATCH_MQ）才「立刻改投死信」。三种写错在客户端本地都表现为「看起来正常」：
    /// 少 +1 ⇒ 毒消息原地转到天荒地老且永远不进死信；-1 读成并发侧的 16 ⇒ 顺序消费凭空多出死信；
    /// 回投成功后仍挂起 ⇒ 队列永久卡死，跟消费者进程死掉一模一样。
    /// 离线单测（OrderlyReconsumeTests.cs）只能锁「回投失败」那一半——未 Start 的内部生产者必败。
    /// </summary>
    private static void ScenarioOrderlyDlq(DefaultMQProducer producer)
    {
        const int maxReconsume = 2;
        const string poison = "ord-poison";
        string topic = _gPrefix + "_OrdDlq";
        string group = _gPrefix + "_g12";
        // 1 队列 + 每批 1 条：ord-after 必须排在毒消息后面，挂起也不会牵连别的路径
        PrepareTopic(producer, topic, 1);

        var listener = new PoisonOrderlyListener(poison);
        var consumer = NewConsumer(group);
        consumer.ConsumeMessageBatchMaxSize = 1;
        consumer.SuspendCurrentQueueTimeMillis = 500;
        consumer.MaxReconsumeTimes = maxReconsume;
        consumer.SetMessageListener(listener);
        consumer.Subscribe(topic, "*");
        consumer.Start();
        Thread.Sleep(3000);
        producer.Send(new Message(topic, Str2Bytes(poison)));
        producer.Send(new Message(topic, Str2Bytes("ord-after")));

        Check("S12-毒消息恰好投 3 次（每次由客户端自己 +1：reconsumeTimes 0/1/2）",
            WaitUntil(() => listener.CountOf(poison) >= 3, 60000)
            && LadderOf(listener, poison),
            "times=[" + TimesOf(listener, poison) + "]");
        // 交棒判据：回投成功后 Java commit 位点，队列必须往前走。少了这一步就是「毒消息把
        // 整个队列钉住」，与消费者死掉无法区分；多了（回投还没成功就前进）则是静默丢消息。
        Check("S12-交给 broker 后业务队列继续前进（后一条被消费）",
            WaitUntil(() => listener.CountOf("ord-after") >= 1, 60000),
            "after=" + listener.CountOf("ord-after").ToString(CultureInfo.InvariantCulture));
        Thread.Sleep(15000);  // 反证：不该有第 4 次
        Check("S12-用尽后不再原地挂起（观察窗口内毒消息只投了 3 次）",
            listener.CountOf(poison) == 3,
            "arrivals=" + listener.CountOf(poison).ToString(CultureInfo.InvariantCulture));
        Check("S12-挂起期间 listener 始终看到业务 topic（本地重投不换 topic）",
            listener.Snapshot().Where(a => a.Body == poison).All(a => a.Topic == topic));
        consumer.Shutdown();

        string dlqTopic = MixAll.GetDlqTopic(group);
        (bool routeFound, List<MessageExt> dlqMsgs) = ReadDlq(group, _gPrefix + "_g12dlq", 40000);
        Check("S12-broker 自动创建并注册了 %DLQ%<group> 路由", routeFound, "dlq=" + dlqTopic);
        // 顺序回投落进死信而不是退回 %RETRY% 重投，本身就是 broker 认定「本组 rebalance 锁
        // 还没过期」⇒ 这个实例真的握着 LOCK_BATCH_MQ（handleRetryAndDLQ:202-207）。
        bool one = dlqMsgs.Count == 1 && Body(dlqMsgs[0]) == poison;
        Check("S12-毒消息落在 %DLQ%<group>（回投走 rebalance 锁，立刻进死信）",
            one, "n=" + dlqMsgs.Count.ToString(CultureInfo.InvariantCulture));
        if (one)
        {
            MessageExt d = dlqMsgs[0];
            // 3 = 客户端在 RECONSUME_TIME 上写的 +1，经 V2 头 j 落成存储值；漏填 j 的话
            // broker 按订阅组默认 16 判，这条永远进不了死信。
            Check("S12-死信 reconsumeTimes = maxReconsumeTimes + 1",
                d.ReconsumeTimes == maxReconsume + 1,
                "reconsumeTimes=" + d.ReconsumeTimes.ToString(CultureInfo.InvariantCulture));
            bool hasRetryTopic = d.Properties.TryGetValue("RETRY_TOPIC", out string? retryTopic);
            Check("S12-死信保留 RETRY_TOPIC=业务 topic，topic 已是 %DLQ%<group>",
                hasRetryTopic && retryTopic == topic && d.Topic == dlqTopic,
                "retryTopic=" + (hasRetryTopic ? retryTopic : "<missing>"));
        }
    }

    /// <summary>
    /// 顺序侧的 -1 是**不设上限**，不是并发侧的 16。Java 两处 getMaxReconsumeTimes 故意不同：
    /// ConsumeMessageOrderlyService:313-320 把 -1 读成 Integer.MAX_VALUE（顺序消费一直在本地
    /// 原地重试，broker 侧没有计数，默认就该重试到成功为止）；DefaultMQPushConsumerImpl:890 把
    /// -1 读成 16（那边每轮都过一遍 broker，16 是 broker 默认的 retryMaxTimes）。合成一个常量
    /// 的两种坏法都得分别挡住：顺序侧读成 16 ⇒ 第 17 次投给 broker，而锁还没过期 ⇒ 直接造出
    /// 一条死信；并发侧读成 MAX ⇒ 毒消息永远不进 %DLQ%（S9 覆盖了「阈值生效」，这条覆盖
    /// 「默认值绝不生效」）。
    /// </summary>
    private static void ScenarioOrderlyNoCap(DefaultMQProducer producer)
    {
        const string poison = "ord-forever";
        string topic = _gPrefix + "_OrdNoCap";
        string group = _gPrefix + "_g12b";
        PrepareTopic(producer, topic, 1);

        var listener = new PoisonOrderlyListener(poison);
        var consumer = NewConsumer(group);
        consumer.ConsumeMessageBatchMaxSize = 1;
        consumer.SuspendCurrentQueueTimeMillis = 200;
        // 显式不设上限（默认就是 -1，写出来是为了让「默认」这条断言有出处）
        consumer.MaxReconsumeTimes = -1;
        consumer.SetMessageListener(listener);
        consumer.Subscribe(topic, "*");
        consumer.Start();
        Thread.Sleep(3000);
        producer.Send(new Message(topic, Str2Bytes(poison)));

        // 只要越过并发侧的 16 就能证明没用错常量。窗口给到 90s：走错的话第 17 次的延迟档位
        // 是 level20（2h），一旦投出去就再也回不来，只能靠「本地投了多少次 + %DLQ% 空」两头夹住。
        Check("S12b-maxReconsumeTimes=-1 时顺序消费不设上限（投过 >=18 次，16 不生效）",
            WaitUntil(() => listener.CountOf(poison) >= 18, 90000)
            && listener.Snapshot().Where(a => a.Body == poison).Max(a => a.ReconsumeTimes) >= 17,
            "arrivals=" + listener.CountOf(poison).ToString(CultureInfo.InvariantCulture)
            + " times=[" + TimesOf(listener, poison) + "]");
        consumer.Shutdown();

        (bool routeFound, List<MessageExt> dlqMsgs) = ReadDlq(group, _gPrefix + "_g12bdlq", 20000);
        Check("S12b-没到阈值就不该有死信（broker 侧连 %DLQ% topic 都不必建）",
            dlqMsgs.Count == 0,
            "routeFound=" + (routeFound ? "true" : "false")
            + " n=" + dlqMsgs.Count.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>命中毒消息时，把 context 上的挂起时长设成指定值并记下本次投递时刻。</summary>
    private sealed class SuspendTimingOrderlyListener : IMessageListenerOrderly
    {
        private readonly string _poison;
        private readonly int _askedMs;
        private readonly object _gate = new();
        private readonly List<long> _stampsMs = new();

        public SuspendTimingOrderlyListener(string poison, int askedMs)
        {
            _poison = poison;
            _askedMs = askedMs;
        }

        public bool Orderly() => true;

        public int Count()
        {
            lock (_gate) return _stampsMs.Count;
        }

        public List<double> GapsSeconds()
        {
            lock (_gate)
            {
                var gaps = new List<double>();
                for (int i = 1; i < _stampsMs.Count; ++i)
                {
                    gaps.Add((_stampsMs[i] - _stampsMs[i - 1]) / 1000.0);
                }

                return gaps;
            }
        }

        public ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext ctx)
        {
            bool hit = msgs.Any(m => Body(m) == _poison);
            if (!hit) return ConsumeOrderlyStatus.Success;
            lock (_gate) _stampsMs.Add(NowMs());
            // 默认 -1；只有显式赋值才走 context 这一支（-1 表示回落消费者配置）
            ctx.SuspendCurrentQueueTimeMillis = _askedMs;
            return ConsumeOrderlyStatus.SuspendCurrentQueueAMoment;
        }
    }

    private static double Median(List<double> xs)
    {
        if (xs.Count == 0) return 0.0;
        var s = new List<double>(xs);
        s.Sort();
        int n = s.Count;
        return n % 2 == 1 ? s[n / 2] : (s[n / 2 - 1] + s[n / 2]) / 2.0;
    }

    /// <summary>
    /// #74：Java ConsumeMessageOrderlyService#submitConsumeRequestLater:211-234 —— `-1`（context
    /// 默认）才回落到消费者配置，解析出的值再钳到 [10, 30000]。真机这三条探针分别证：
    /// context 70ms 压过配置 900ms（中位间隔贴着 70ms）、context 保持 -1 时回落配置 400ms、
    /// 两个非法值（context 1ms / 配置 0）钳到下限不忙等。
    /// 钳位端点只能兜「不忙等」（>= 10ms）：分发循环本身有 50ms 固定节拍，间隔法分不出 10ms
    /// 与 1ms —— 精确值由离线矩阵锁死（OrderlyReconsumeTests.OrderlySuspendMillisResolvesThenClampsLikeJava）。
    /// </summary>
    private static void ScenarioOrderlySuspendMillis(DefaultMQProducer producer)
    {
        SuspendProbe(producer, "OrdSusp", "g12c", configMs: 900, askedMs: 70, poison: "ord-susp-poison",
            need: 9, minGaps: 6, timeoutMs: 25000, lo: 0.03, hi: 0.4,
            name: "S12c-context 的 70ms 生效（不是消费者配置的 900ms）");
        SuspendProbe(producer, "OrdSuspCfg", "g12ccfg", configMs: 400, askedMs: -1, poison: "ord-susp-cfg",
            need: 6, minGaps: 4, timeoutMs: 25000, lo: 0.25, hi: 0.75,
            name: "S12c-context 保持 -1 时回落消费者配置的 400ms");
        SuspendProbe(producer, "OrdSuspFloor", "g12cfloor", configMs: 0, askedMs: 1, poison: "ord-susp-floor",
            need: 9, minGaps: 6, timeoutMs: 20000, lo: 0.009, hi: 10.0,
            name: "S12c-钳位下限：context 1ms / 配置 0 时不忙等（间隔 >= 10ms）");
    }

    private static void SuspendProbe(DefaultMQProducer producer, string suffix, string groupSuffix,
        int configMs, int askedMs, string poison, int need, int minGaps, int timeoutMs, double lo,
        double hi, string name)
    {
        string topic = _gPrefix + "_" + suffix;
        string group = _gPrefix + "_" + groupSuffix;
        PrepareTopic(producer, topic, 1);  // 1 队列：同一队列的顺序重投才有可比间隔

        var listener = new SuspendTimingOrderlyListener(poison, askedMs);
        var consumer = NewConsumer(group);
        consumer.ConsumeMessageBatchMaxSize = 1;
        consumer.MaxReconsumeTimes = -1;  // 顺序侧不设上限，才够采到足够多的间隔
        consumer.SuspendCurrentQueueTimeMillis = configMs;
        consumer.SetMessageListener(listener);
        consumer.Subscribe(topic, "*");
        consumer.Start();
        // 等首轮 LOCK_BATCH_MQ：没拿到队列锁时 broker 会把顺序重投直接改投死信，间隔就没了
        Thread.Sleep(4000);
        producer.Send(new Message(topic, Str2Bytes(poison)));
        WaitUntil(() => listener.Count() >= need, timeoutMs);
        List<double> gaps = listener.GapsSeconds();
        double med = Median(gaps);
        Check(name, gaps.Count >= minGaps && med >= lo && med <= hi,
            "n=" + listener.Count().ToString(CultureInfo.InvariantCulture)
            + " median=" + med.ToString("F3", CultureInfo.InvariantCulture) + "s");
        consumer.Shutdown();
    }

    // ---------------- S13 顺序侧显式批量 ack（COMMIT）/ 显式回滚（ROLLBACK）----------------

    private enum ManualMode
    {
        CommitOnce,          // autoCommit=false + COMMIT：显式整批认可
        RollbackThenCommit,  // autoCommit=false + ROLLBACK：退回本地重投若干次，再整批认可
        IllegalRollback,     // autoCommit 保持 true 却返回 ROLLBACK：非法用法，按 ack 处理
    }

    private sealed class ManualOrderlyListener : IMessageListenerOrderly
    {
        private readonly ManualMode _mode;
        private readonly string _head;
        private readonly int _rollbackCalls;
        private readonly object _gate = new();
        private readonly List<List<string>> _batches = new();
        private readonly List<(string Body, int ReconsumeTimes, string Topic, long Ts)> _seen = new();
        private int _headCalls;

        public ManualOrderlyListener(ManualMode mode, string head, int rollbackCalls)
        {
            _mode = mode;
            _head = head;
            _rollbackCalls = rollbackCalls;
        }

        public bool Orderly() => true;

        public List<List<string>> Batches()
        {
            lock (_gate) return new List<List<string>>(_batches);
        }

        public List<(string Body, int ReconsumeTimes, string Topic, long Ts)> Snapshot()
        {
            lock (_gate) return new List<(string Body, int ReconsumeTimes, string Topic, long Ts)>(_seen);
        }

        public int Count(string body)
        {
            lock (_gate) return _seen.Count(r => r.Body == body);
        }

        public int FirstIndex(string body)
        {
            lock (_gate)
            {
                for (int i = 0; i < _seen.Count; ++i)
                {
                    if (_seen[i].Body == body) return i;
                }

                return -1;
            }
        }

        public List<double> HeadGaps(string body)
        {
            lock (_gate)
            {
                var stamps = _seen.Where(r => r.Body == body).Select(r => r.Ts).ToList();
                var gaps = new List<double>();
                for (int i = 1; i < stamps.Count; ++i)
                {
                    gaps.Add((stamps[i] - stamps[i - 1]) / 1000.0);
                }

                return gaps;
            }
        }

        public ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext ctx)
        {
            bool hit = msgs.Any(m => Body(m) == _head);
            int n;
            lock (_gate)
            {
                _batches.Add(msgs.Select(Body).ToList());
                foreach (MessageExt m in msgs)
                {
                    _seen.Add((Body(m), m.ReconsumeTimes, m.Topic, NowMs()));
                }

                if (hit) ++_headCalls;
                n = _headCalls;
            }

            if (_mode == ManualMode.CommitOnce)
            {
                ctx.AutoCommit = false;
                return ConsumeOrderlyStatus.Commit;
            }

            if (_mode == ManualMode.RollbackThenCommit)
            {
                ctx.AutoCommit = false;
                return hit && n <= _rollbackCalls
                    ? ConsumeOrderlyStatus.Rollback
                    : ConsumeOrderlyStatus.Commit;
            }

            // 非法用法：不碰 AutoCommit（保持默认 true），只改返回值
            return ConsumeOrderlyStatus.Rollback;
        }
    }

    /// <summary>
    /// S13：Java 顺序消费 processConsumeResult 的显式分支（ConsumeMessageOrderlyService:270-300）。
    /// 三条探针的判别力都在"本地 vs broker"上：真回滚是 ProcessQueue 本地重投（间隔贴着挂起
    /// 时长、reconsumeTimes 不动），任何"交给 broker 走 %RETRY%"的实现最快也是 delayLevel=3
    /// 的 10s 档 —— 间隔量级差 40 倍以上，真机完全可分。
    /// </summary>
    private static void ScenarioOrderlyAckRollback(DefaultMQProducer producer)
    {
        ManualAckCommitProbe(producer);
        ManualRollbackProbe(producer);
        ManualIllegalRollbackProbe(producer);
    }

    private static long ReadCommitted(DefaultMQAdminExt admin, MessageQueue mq, string group)
    {
        try
        {
            return admin.ExamineConsumerOffset(group, mq, out long off) ? off : -1;
        }
        catch (Exception)
        {
            return -1;
        }
    }

    /// <summary>S13a autoCommit=false + COMMIT：整批 3 条一次认可（Java:275-277）。</summary>
    private static void ManualAckCommitProbe(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_OrdCommit";
        string group = _gPrefix + "_g13a";
        PrepareTopic(producer, topic, 1);

        var listener = new ManualOrderlyListener(ManualMode.CommitOnce, "cm-none", 0);
        var c = NewConsumer(group);
        c.ConsumeMessageBatchMaxSize = 3;
        // 新组 LAST_OFFSET 会从分配时刻起算，先放 3 条再起消费者 + 显式从 0 起消，
        // 首次拉取才正好是「一整批 3 条」（同 S10 的口径）
        c.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
        c.SetMessageListener(listener);
        c.Subscribe(topic, "*");
        for (int i = 0; i < 3; ++i)
        {
            producer.Send(new Message(topic, Str2Bytes("cm-" + i.ToString(CultureInfo.InvariantCulture))));
        }

        c.Start();
        WaitUntil(() => listener.Batches().Count >= 1, 30000);
        Thread.Sleep(6000);  // 反证窗口：COMMIT 若被当成"不提交"，200ms 挂起节奏下 6s 会重投 ~30 次
        List<List<string>> batches = listener.Batches();
        bool firstBatch3 = batches.Count > 0 && batches[0].Count == 3;
        Check("S13a-首批就是整批 3 条（COMMIT 一次认可整批）",
            firstBatch3,
            "firstBatch=" + (batches.Count > 0 ? batches[0].Count.ToString(CultureInfo.InvariantCulture) : "none"));
        Check("S13a-6 秒里只投了这一批（COMMIT 不是被当成 SUCCESS 的不提交）",
            batches.Count == 1,
            "batches=" + batches.Count.ToString(CultureInfo.InvariantCulture));
        Check("S13a-3 条各只投一次（整批 ack 未回投）",
            listener.Count("cm-0") == 1 && listener.Count("cm-1") == 1 && listener.Count("cm-2") == 1,
            "deliveries=" + (listener.Count("cm-0") + listener.Count("cm-1") + listener.Count("cm-2"))
                .ToString(CultureInfo.InvariantCulture));

        List<MessageQueue> queues = c.FetchSubscribeMessageQueues(topic);
        c.Shutdown();
        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_namesrv);
        admin.SetTimeoutMillis(10000);
        try
        {
            admin.Start();
            if (queues.Count > 0)
            {
                MessageQueue mq = queues[0];
                WaitUntil(() => ReadCommitted(admin, mq, group) == 3, 30000);
                Check("S13a-broker 位点一次前进到批尾 3（整批 ack，不是卡在 0）",
                    ReadCommitted(admin, mq, group) == 3,
                    "committed=" + ReadCommitted(admin, mq, group).ToString(CultureInfo.InvariantCulture));
            }
            else
            {
                Check("S13a-业务 topic 有队列可查位点", false, "queues=0");
            }
        }
        catch (Exception e)
        {
            Check("S13a-位点查询可用", false, e.Message);
        }

        admin.Shutdown();
    }

    /// <summary>S13b autoCommit=false + ROLLBACK：显式回滚、本地立即重投（Java:278-285）。</summary>
    private static void ManualRollbackProbe(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_OrdRollback";
        string group = _gPrefix + "_g13b";
        PrepareTopic(producer, topic, 1);

        var listener = new ManualOrderlyListener(ManualMode.RollbackThenCommit, "rb-head", 6);
        var c = NewConsumer(group);
        c.ConsumeMessageBatchMaxSize = 1;
        c.MaxReconsumeTimes = -1;  // 顺序侧不设上限，免得本地重投被判成毒消息
        c.SuspendCurrentQueueTimeMillis = 200;
        c.SetMessageListener(listener);
        c.Subscribe(topic, "*");
        c.Start();
        // 等首轮 LOCK_BATCH_MQ：没拿到队列锁时 broker 会把顺序重投直接改投死信
        Thread.Sleep(4000);
        producer.Send(new Message(topic, Str2Bytes("rb-head")));
        producer.Send(new Message(topic, Str2Bytes("rb-next")));

        bool arrived = WaitUntil(() => listener.Count("rb-head") >= 7 && listener.FirstIndex("rb-next") >= 0, 45000);
        Thread.Sleep(2000);  // 反证窗口：退回重投只该按提交次数收尾，多余的投递会露出来
        List<double> gaps = listener.HeadGaps("rb-head");
        double med = Median(gaps);
        double worst = gaps.Count > 0 ? gaps.Max() : 0;
        var headRecords = listener.Snapshot().Where(r => r.Body == "rb-head").ToList();
        // 第 7 次投递在整条序列里的下标（idx7 之前的 head 投递都不算数）
        int idx7 = -1;
        int seenHead = 0;
        List<(string Body, int ReconsumeTimes, string Topic, long Ts)> all = listener.Snapshot();
        for (int i = 0; i < all.Count; ++i)
        {
            if (all[i].Body == "rb-head" && ++seenHead == 7)
            {
                idx7 = i;
                break;
            }
        }

        int idxNext = listener.FirstIndex("rb-next");

        Check("S13b-显式回滚把同一批退回重投（head 投递 7 次 = 6 次回滚 + 1 次提交）",
            arrived && listener.Count("rb-head") == 7,
            "deliveries=" + listener.Count("rb-head").ToString(CultureInfo.InvariantCulture));
        Check("S13b-本地重投不过 broker：相邻间隔贴着 200ms 挂起（%RETRY% 最快 10s 档）",
            gaps.Count >= 5 && med < 1.0 && worst < 5.0,
            "n=" + gaps.Count.ToString(CultureInfo.InvariantCulture)
            + " median=" + med.ToString("F3", CultureInfo.InvariantCulture)
            + "s max=" + worst.ToString("F3", CultureInfo.InvariantCulture) + "s");
        Check("S13b-本地重投不动 reconsumeTimes、不换 topic（没有走 %RETRY%）",
            headRecords.Count > 0 && headRecords.All(r => r.ReconsumeTimes == 0 && r.Topic == topic),
            "times=" + string.Join(",",
                headRecords.Select(r => r.ReconsumeTimes.ToString(CultureInfo.InvariantCulture))));
        Check("S13b-已提交前 next 不越位（回滚期间后面的消息不放行）",
            idx7 >= 0 && idxNext > idx7,
            "idx7=" + idx7.ToString(CultureInfo.InvariantCulture)
            + " idxNext=" + idxNext.ToString(CultureInfo.InvariantCulture));
        Check("S13b-收尾后 head 恰好 7 次、next 恰好 1 次",
            listener.Count("rb-head") == 7 && listener.Count("rb-next") == 1,
            "head=" + listener.Count("rb-head").ToString(CultureInfo.InvariantCulture)
            + " next=" + listener.Count("rb-next").ToString(CultureInfo.InvariantCulture));

        List<MessageQueue> queues = c.FetchSubscribeMessageQueues(topic);
        c.Shutdown();
        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_namesrv);
        admin.SetTimeoutMillis(10000);
        try
        {
            admin.Start();
            if (queues.Count > 0)
            {
                MessageQueue mq = queues[0];
                WaitUntil(() => ReadCommitted(admin, mq, group) == 2, 30000);
                Check("S13b-回滚不提前提交位点，提交后位点到 2",
                    ReadCommitted(admin, mq, group) == 2,
                    "committed=" + ReadCommitted(admin, mq, group).ToString(CultureInfo.InvariantCulture));
            }
            else
            {
                Check("S13b-业务 topic 有队列可查位点", false, "queues=0");
            }
        }
        catch (Exception e)
        {
            Check("S13b-位点查询可用", false, e.Message);
        }

        admin.Shutdown();
    }

    /// <summary>
    /// S13c autoCommit=true + ROLLBACK = 非法用法（Java:246-250 只 warn）⇒ 顺势落进 SUCCESS 分支
    /// 按 ack 处理。真按回滚办的话 head 会被挂起节奏反复投递、next 永远不放行 —— 两种行为完全可分。
    /// </summary>
    private static void ManualIllegalRollbackProbe(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_OrdIllegal";
        string group = _gPrefix + "_g13c";
        PrepareTopic(producer, topic, 1);

        var listener = new ManualOrderlyListener(ManualMode.IllegalRollback, "il-none", 0);
        var c = NewConsumer(group);
        c.ConsumeMessageBatchMaxSize = 1;
        c.SuspendCurrentQueueTimeMillis = 200;
        c.SetMessageListener(listener);
        c.Subscribe(topic, "*");
        c.Start();
        Thread.Sleep(4000);
        producer.Send(new Message(topic, Str2Bytes("il-head")));
        producer.Send(new Message(topic, Str2Bytes("il-next")));

        WaitUntil(() => listener.FirstIndex("il-next") >= 0, 30000);
        Thread.Sleep(2500);  // 反证窗口：真按回滚办的话这 2.5s 里 head 会被重投 ~10 次
        Check("S13c-autoCommit=true 时 ROLLBACK 按 ack 处理：head 只投一次",
            listener.Count("il-head") == 1,
            "head=" + listener.Count("il-head").ToString(CultureInfo.InvariantCulture));
        Check("S13c-非法 ROLLBACK 不阻塞队列：next 立刻被消费",
            listener.FirstIndex("il-next") >= 0,
            "idxNext=" + listener.FirstIndex("il-next").ToString(CultureInfo.InvariantCulture)
            + " next=" + listener.Count("il-next").ToString(CultureInfo.InvariantCulture));

        List<MessageQueue> queues = c.FetchSubscribeMessageQueues(topic);
        c.Shutdown();
        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_namesrv);
        admin.SetTimeoutMillis(10000);
        try
        {
            admin.Start();
            if (queues.Count > 0)
            {
                MessageQueue mq = queues[0];
                WaitUntil(() => ReadCommitted(admin, mq, group) == 2, 30000);
                Check("S13c-broker 位点前进到 2（非法用法按成功 ack）",
                    ReadCommitted(admin, mq, group) == 2,
                    "committed=" + ReadCommitted(admin, mq, group).ToString(CultureInfo.InvariantCulture));
            }
            else
            {
                Check("S13c-业务 topic 有队列可查位点", false, "queues=0");
            }
        }
        catch (Exception e)
        {
            Check("S13c-位点查询可用", false, e.Message);
        }

        admin.Shutdown();
    }

    // ---------------- S14 correctTagsOffset（空应答修正已消费位点）----------------

    /// <summary>
    /// Java <c>DefaultMQPushConsumerImpl#correctTagsOffset:713-717</c>（调用点 <c>:394-401</c>）：
    /// 拉取应答是 NO_NEW_MSG / NO_MATCHED_MSG 时，这条队列的"已消费位点"必须跟着拉取游标走，
    /// 否则没人 ack 的消息（broker 过滤掉的不在应答里、客户端二次过滤摘掉的明确不 ack）
    /// 会让位点永久卡死。离线单测锁得住「哪些状态要修正 + 闸门何时放行」，锁不住
    /// 「这条修正真的走到了 broker」—— 位点最终由 UPDATE_CONSUMER_OFFSET 落盘，只有真集群
    /// 能证明 broker 上的已提交位点前移了、而且是在**一条消息都没投递**的前提下前移的。
    /// </summary>
    private static void ScenarioCorrectTagsOffset(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_Cto";
        string ctrlGroup = _gPrefix + "_g14ctrl";
        string testGroup = _gPrefix + "_g14";
        PrepareTopic(producer, topic, 4);

        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_namesrv);
        admin.SetTimeoutMillis(10000);
        admin.Start();

        // ---------- S14a 对照组：消息确实在，且常规消费的落点就是各队列 maxOffset ----------
        var ctrlListener = new CollectingListenerConcurrently();
        var ctrl = NewConsumer(ctrlGroup);
        ctrl.ConsumeMessageBatchMaxSize = 3;
        ctrl.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
        ctrl.SetMessageListener(ctrlListener);
        ctrl.Subscribe(topic, "TagA");
        ctrl.Start();
        List<MessageQueue> mainQueues = ctrl.FetchSubscribeMessageQueues(topic);
        Check("S14a-业务 topic 有 4 个队列可查位点", mainQueues.Count == 4,
            "queues=" + mainQueues.Count.ToString(CultureInfo.InvariantCulture));
        Thread.Sleep(2000);
        for (int i = 0; i < 5; ++i)
        {
            producer.Send(new Message(topic, "TagA", string.Empty,
                Str2Bytes("cto-" + i.ToString(CultureInfo.InvariantCulture))));
        }

        bool ctrlOk = WaitUntil(() => ctrlListener.Snapshot().Count >= 5, 30000);
        Check("S14a-对照组（TagA）收齐 5 条 —— 消息确实在队列里",
            ctrlOk && ctrlListener.Snapshot().Count == 5,
            "arrivals=" + ctrlListener.Snapshot().Count.ToString(CultureInfo.InvariantCulture));

        // committed 用 -1 表示 broker 查无记录（QUERY_NOT_FOUND），0 是合法位点，两者不能混同
        List<(int QueueId, long Max, long Committed)> Snap(List<MessageQueue> qs, string group)
        {
            var rows = new List<(int, long, long)>();
            foreach (MessageQueue mq in qs)
            {
                long maxOf = -1;
                try
                {
                    maxOf = admin.MaxOffset(mq);
                }
                catch (Exception)
                {
                    maxOf = -1;
                }

                long off = -1;
                try
                {
                    if (admin.ExamineConsumerOffset(group, mq, out long o)) off = o;
                }
                catch (Exception)
                {
                    off = -1;
                }

                rows.Add((mq.QueueId, maxOf, off));
            }

            return rows;
        }

        string Text(List<(int QueueId, long Max, long Committed)> rows) =>
            string.Join(" ", rows.Select(r => "q" + r.QueueId.ToString(CultureInfo.InvariantCulture)
                + ":" + r.Committed.ToString(CultureInfo.InvariantCulture)
                + "/" + r.Max.ToString(CultureInfo.InvariantCulture)));

        bool Matches(List<MessageQueue> qs, string group) =>
            Snap(qs, group).All(r => r.Committed >= 0 && r.Committed == r.Max);

        WaitUntil(() => Matches(mainQueues, ctrlGroup), 25000);
        List<(int QueueId, long Max, long Committed)> ctrlRows = Snap(mainQueues, ctrlGroup);
        Check("S14a-对照组的已提交位点 == 各队列 maxOffset（数值口径）",
            Matches(mainQueues, ctrlGroup), Text(ctrlRows));
        Check("S14a-对照组确实把消息推进了队列（maxOffset 总和 > 0）",
            ctrlRows.Sum(r => r.Max) > 0,
            "maxSum=" + ctrlRows.Sum(r => r.Max).ToString(CultureInfo.InvariantCulture));
        ctrl.Shutdown();

        // ---------- S14b NO_MATCHED_MSG：永不匹配的订阅，零投递但位点要走 ----------
        var testListener = new CollectingListenerConcurrently();
        var test = NewConsumer(testGroup);
        test.ConsumeMessageBatchMaxSize = 3;
        test.SetMessageListener(testListener);
        test.Subscribe(topic, "TagB");
        test.Start();
        // 首跳 10s + 周期 5s 的位点持久化节拍，窗口给足
        WaitUntil(() => Matches(mainQueues, testGroup), 40000);
        List<(int QueueId, long Max, long Committed)> testRows = Snap(mainQueues, testGroup);
        Check("S14b-零投递（listener 一条都没收到）",
            testListener.Snapshot().Count == 0,
            "arrivals=" + testListener.Snapshot().Count.ToString(CultureInfo.InvariantCulture));
        Check("S14b-每条队列的已提交位点都 == 该队列 maxOffset（空应答修正生效）",
            Matches(mainQueues, testGroup), Text(testRows));
        Check("S14b-修正后的位点总和 == 对照组（同一条队列的最大位点）",
            testRows.Sum(r => r.Committed) == ctrlRows.Sum(r => r.Max),
            "test=" + testRows.Sum(r => r.Committed).ToString(CultureInfo.InvariantCulture)
            + " ctrl=" + ctrlRows.Sum(r => r.Max).ToString(CultureInfo.InvariantCulture));

        // ---------- S14c NO_NEW_MSG：%RETRY%<group> 空队列也要留下位点记录 ----------
        string retryTopic = MixAll.GetRetryTopic(testGroup);
        var probe = new MQClientInstance("ctoprobe-" + NowMs().ToString(CultureInfo.InvariantCulture),
            new List<string> { _namesrv });
        probe.Start();
        List<MessageQueue> retryQueues = new();
        TopicRouteData? retryRoute = null;
        WaitUntil(() =>
        {
            retryRoute = probe.GetTopicRouteData(retryTopic);
            if (retryRoute == null || retryRoute.QueueDatas.Count == 0) return false;
            retryQueues.Clear();
            foreach (QueueData q in retryRoute.QueueDatas)
            {
                for (int i = 0; i < q.ReadQueueNums; ++i)
                {
                    retryQueues.Add(new MessageQueue(retryTopic, q.BrokerName, i));
                }
            }

            return retryQueues.Count > 0 && Matches(retryQueues, testGroup);
        }, 40000);
        List<(int QueueId, long Max, long Committed)> retryRows = Snap(retryQueues, testGroup);
        Check("S14c-" + retryTopic + " 上出现位点记录且等于 maxOffset",
            retryQueues.Count > 0 && Matches(retryQueues, testGroup), Text(retryRows));
        Check("S14c-该位点确实是 0（空队列的 nextBeginOffset）",
            retryRows.Count > 0 && retryRows.All(r => r.Committed == 0),
            "offsets=" + string.Join(",",
                retryRows.Select(r => r.Committed.ToString(CultureInfo.InvariantCulture))));

        // 再等一个静默窗口：修正只抬位点、不该投递任何东西
        Thread.Sleep(6000);
        Check("S14c-整轮下来 listener 依旧是 0 条（修正不会凭空投递）",
            testListener.Snapshot().Count == 0,
            "arrivals=" + testListener.Snapshot().Count.ToString(CultureInfo.InvariantCulture));

        probe.Shutdown();
        test.Shutdown();
        admin.Shutdown();
    }

    private static bool LadderOf(PoisonOrderlyListener listener, string body)
    {
        List<int> times = listener.Snapshot()
            .Where(a => a.Body == body).Select(a => a.ReconsumeTimes).ToList();
        return times.Count >= 3 && times[0] == 0 && times[1] == 1 && times[2] == 2;
    }

    private static string TimesOf(PoisonOrderlyListener listener, string body) =>
        string.Join(",", listener.Snapshot()
            .Where(a => a.Body == body).Select(a => a.ReconsumeTimes.ToString(CultureInfo.InvariantCulture)));

    /// <summary>
    /// %DLQ%&lt;group&gt; 现场取证：先用一次性 probe 等 broker 把死信 topic 注册进路由，
    /// 再用**独立消费组 + FirstOffset** 把已有的死信读出来（新组 + LAST 会从队尾开始，
    /// 把已经落在死信里的那条直接跳过 ⇒ 假失败）。routeFound 单独返回：负向用例里
    /// 「broker 压根没建死信 topic」本身就是正确结论，不能和「等不到路由」混成同一个空结果。
    /// </summary>
    private static (bool RouteFound, List<MessageExt> Msgs) ReadDlq(string group, string readerGroup,
        int timeoutMs)
    {
        string dlqTopic = MixAll.GetDlqTopic(group);
        TopicRouteData? route = null;
        using (var probe = new MQClientInstance("dlqprobe-" + NowMs().ToString(CultureInfo.InvariantCulture),
                   new List<string> { _namesrv }))
        {
            probe.Start();
            long deadline = NowMs() + timeoutMs;
            while (NowMs() < deadline)
            {
                route = probe.GetTopicRouteData(dlqTopic);
                if (route != null && route.QueueDatas.Count > 0) break;
                route = null;
                Thread.Sleep(2000);
            }
        }

        if (route == null) return (false, new List<MessageExt>());

        var queues = new List<MessageQueue>();
        foreach (QueueData q in route.QueueDatas)
        {
            for (int i = 0; i < q.ReadQueueNums; ++i)
            {
                queues.Add(new MessageQueue(dlqTopic, q.BrokerName, i));
            }
        }

        var msgs = new List<MessageExt>();
        var reader = new DefaultLitePullConsumer(readerGroup);
        reader.SetNamesrvAddr(_namesrv);
        reader.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        reader.Assign(queues);
        reader.Start();
        foreach (MessageQueue mq in queues) reader.SeekToBegin(mq);
        // SeekToBegin 必须在 Start 之后（未 Start 会抛），它把拉取游标拨回最小位点；
        // 若拉取循环抢先把同一格拉进了本地缓冲，Seek 只丢「offset 之前」的副本，同一个
        // 消息会以同一 (QueueId, QueueOffset) 交付两次。按存储位置去重：真有一式两份
        // 入死信，副本落在不同的 QueueOffset 上，不会被吃掉。
        var seenAt = new HashSet<(int QueueId, long QueueOffset)>();
        void Collect(IEnumerable<MessageExt> batch)
        {
            foreach (MessageExt m in batch)
            {
                if (seenAt.Add((m.QueueId, m.QueueOffset))) msgs.Add(m);
            }
        }

        long pollDeadline = NowMs() + timeoutMs;
        while (NowMs() < pollDeadline)
        {
            Collect(reader.Poll(1000));
            if (msgs.Count == 0) continue;
            // 收到后再排空几趟：断言「死信里只有这一条」要求把后面的也看见，
            // 但总窗口必须有界，否则正向用例每次都要白等满 timeoutMs。
            long drainTo = NowMs() + 3000;
            while (NowMs() < drainTo) Collect(reader.Poll(500));
            break;
        }
        reader.Shutdown();
        return (true, msgs);
    }
}
