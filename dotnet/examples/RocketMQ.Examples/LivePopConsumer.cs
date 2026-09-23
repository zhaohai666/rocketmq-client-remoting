// POP **消费侧**真机验证（对齐 Java ConsumeMessagePopConcurrentlyService）。
// 用法：rmq popc [namesrv]
//
// 与 rmq pop（协议管道）的区别：这里验证**消费循环** ——
//   POP 弹出 → 投递 listener → 成功则 ack、失败则延长不可见时间 → 不重复投递。
// 消费循环不查/不提交消费位点，进度完全由 broker 的 checkpoint 跟踪。
//
// 场景（与 python/verify_pop_consumer_live.py、cpp/examples/live_pop_consumer.cpp 同套）：
//   S1 POP 消费：起消费者 → 发 12 条 → 全部收到、无重复、body 集合一致
//   S2 ack 生效：收满后再观察一段时间（> invisibleTime）→ 不应被重复投递
//   S3 RECONSUME_LATER：listener 持续返回 RECONSUME_LATER → 按延迟档位重新投递
//   S4 多队列：消息确实落到了多个队列且都被消费（POP 是逐队列弹的）
//   S5 拉取统计：POP 路径也要把 pullRT/pullTPS 记进 307 状态表（持续流量 + 真实 RPC）
//
// ⚠ S2 的观测窗口必须 > PopInvisibleTime，否则"ack 完全没发出去"也看不出重复投递
//   （消息还没到复活时间）——这是最容易伪装成通过的假绿。
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LivePopConsumer
{
    private static readonly string Stamp =
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    private static int _pass;
    private static int _fail;

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok)
        {
            _pass++;
            Console.WriteLine("  [PASS] " + name + (detail.Length == 0 ? "" : "  " + detail));
        }
        else
        {
            _fail++;
            Console.WriteLine("  [FAIL] " + name + (detail.Length == 0 ? "" : "  " + detail));
        }
    }

    private static bool WaitUntil(Func<bool> pred, int timeoutMs)
    {
        long deadline = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + timeoutMs;
        while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(200);
        }

        return pred();
    }

    private static void PrepareTopic(DefaultMQProducer producer, string topic)
    {
        try
        {
            producer.CreateTopic("TBW102", topic, 4);
        }
        catch (Exception e)
        {
            Console.WriteLine("  !! CreateTopic(" + topic + ") failed: " + e.Message);
        }
    }

    private sealed class SuccessListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<string> _bodies = new();
        private readonly HashSet<int> _queues = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                foreach (MessageExt m in msgs)
                {
                    _bodies.Add(Encoding.UTF8.GetString(m.Body));
                    _queues.Add(m.QueueId);
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public List<string> Snapshot()
        {
            lock (_lk) return new List<string>(_bodies);
        }

        public int QueueCount()
        {
            lock (_lk) return _queues.Count;
        }
    }

    private sealed class LaterListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<string> _keys = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                foreach (MessageExt m in msgs)
                {
                    string k = m.GetProperty(MessageConst.PropertyKeys) ?? string.Empty;
                    _keys.Add(k.Length == 0 ? m.MsgId : k);
                }
            }

            return ConsumeConcurrentlyStatus.ReconsumeLater;
        }

        public int Count()
        {
            lock (_lk) return _keys.Count;
        }

        public List<string> Snapshot()
        {
            lock (_lk) return new List<string>(_keys);
        }

        // 至少被投递过两次的不同消息数（"每条都重投了"才是 S3 要证明的语义）
        public int RedeliveredKinds()
        {
            lock (_lk) return _keys.GroupBy(k => k).Count(g => g.Count() >= 2);
        }
    }

    public static int Run(string[] args)
    {
        string nsAddr = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        string topic = "PopConsNet_" + Stamp;
        string group = "GID_PopConsNet_" + Stamp;
        string topicLater = "PopConsNetLater_" + Stamp;
        string groupLater = "GID_PopConsNetLater_" + Stamp;
        string topicStats = "PopConsNetStats_" + Stamp;
        string groupStats = "GID_PopConsNetStats_" + Stamp;
        const int nMsg = 12;

        Console.WriteLine("=".PadRight(70, '='));
        Console.WriteLine("POP consumer live (.NET): namesrv=" + nsAddr + " topic=" + topic);
        Console.WriteLine("=".PadRight(70, '='));

        var prep = new DefaultMQProducer("PG_PopConsNetPrep_" + Stamp);
        prep.NamesrvAddr = nsAddr;
        prep.Start();
        PrepareTopic(prep, topic);
        PrepareTopic(prep, topicLater);
        PrepareTopic(prep, topicStats);
        prep.Shutdown();

        // ---------------- S1 / S2 / S4：正常消费 + ack ----------------
        Console.WriteLine("=== S1 POP 消费（全收 + 无重复）===");
        var listener = new SuccessListener();
        var consumer = new DefaultMQPushConsumer(group);
        consumer.SetNamesrvAddr(nsAddr);
        consumer.PopMode = true;
        consumer.SetConsumeThreadNums(4);
        consumer.ConsumeMessageBatchMaxSize = 4;
        // ⚠ 故意压到 10s：让"没 ack → invisibleTime 到期复活重投"在观测窗口内来得及暴露
        consumer.PopInvisibleTime = 10000;
        consumer.SetMessageListener(listener);
        consumer.Subscribe(topic, "*");
        // 先起消费者再发消息（见项目约定）
        consumer.Start();
        Thread.Sleep(1000);

        var sent = new HashSet<string>(StringComparer.Ordinal);
        var producer = new DefaultMQProducer("PG_PopConsNet_" + Stamp);
        producer.NamesrvAddr = nsAddr;
        producer.Start();
        for (int i = 0; i < nMsg; i++)
        {
            string body = "pop-cons-" + i.ToString("00", CultureInfo.InvariantCulture);
            var msg = new Message(topic, Encoding.UTF8.GetBytes(body));
            msg.Keys = "pc" + i.ToString(CultureInfo.InvariantCulture);
            producer.Send(msg);
            sent.Add(body);
        }

        Console.WriteLine("  sent " + nMsg.ToString(CultureInfo.InvariantCulture) + " msgs");

        bool all = WaitUntil(() => listener.Snapshot().Count >= nMsg, 30000);
        // 必须 > PopInvisibleTime(10s)，否则 ack 没发也看不出重复投递
        Thread.Sleep(16000);

        List<string> got = listener.Snapshot();
        var gotSet = new HashSet<string>(got, StringComparer.Ordinal);
        Check("S1a 全部消息被消费", all && got.Count >= nMsg,
            "received=" + got.Count.ToString(CultureInfo.InvariantCulture)
                + "/" + nMsg.ToString(CultureInfo.InvariantCulture));
        Check("S1b body 集合与发送一致", gotSet.SetEquals(sent),
            "got=" + gotSet.Count.ToString(CultureInfo.InvariantCulture)
                + " sent=" + sent.Count.ToString(CultureInfo.InvariantCulture));
        Check("S2a 无重复投递", gotSet.Count == got.Count,
            "received=" + got.Count.ToString(CultureInfo.InvariantCulture)
                + " unique=" + gotSet.Count.ToString(CultureInfo.InvariantCulture));
        Check("S2b 观察期内没有新增投递", got.Count == nMsg,
            "received=" + got.Count.ToString(CultureInfo.InvariantCulture));
        Check("S4 消息分布在多个队列且都被消费", listener.QueueCount() > 1,
            "queues=" + listener.QueueCount().ToString(CultureInfo.InvariantCulture));
        consumer.Shutdown();
        producer.Shutdown();

        // ---------------- S3：RECONSUME_LATER → 延迟后重投 ----------------
        Console.WriteLine("=== S3 RECONSUME_LATER → 延迟后重投 ===");
        var laterListener = new LaterListener();
        var c2 = new DefaultMQPushConsumer(groupLater);
        c2.SetNamesrvAddr(nsAddr);
        c2.PopMode = true;
        c2.SetConsumeThreadNums(2);
        c2.PopInvisibleTime = 5000;
        // 消费失败时的延迟档位：把第一档压到 3s，让"延长不可见 → 重新可见"尽快发生
        c2.PopDelayLevel = new List<int> { 3, 10, 30, 60, 120, 300, 600, 1200, 1800, 3600, 7200 };
        c2.SetMessageListener(laterListener);
        c2.Subscribe(topicLater, "*");
        c2.Start();
        Thread.Sleep(1000);

        var p2 = new DefaultMQProducer("PG_PopConsNetLater_" + Stamp);
        p2.NamesrvAddr = nsAddr;
        p2.Start();
        for (int i = 0; i < 3; i++)
        {
            var msg = new Message(topicLater, Encoding.UTF8.GetBytes("later-" + i.ToString(CultureInfo.InvariantCulture)));
            msg.Keys = "pl" + i.ToString(CultureInfo.InvariantCulture);
            p2.Send(msg);
        }

        bool first = WaitUntil(() => laterListener.Count() >= 3, 30000);
        int firstRound = laterListener.Count();
        Check("S3a 首轮投递 3 条", first && firstRound >= 3,
            "count=" + firstRound.ToString(CultureInfo.InvariantCulture));

        // ⚠ 断言的是"每条都重投过"，不是"又多收了 3 次投递"：按条数算时，
        // 某一条被重投 3 次而另一条从没回来也会通过。
        bool again = WaitUntil(() => laterListener.RedeliveredKinds() >= 3, 40000);
        Check("S3b 每条消费失败的消息都被重新投递（延长不可见时间生效）", again,
            "first=" + firstRound.ToString(CultureInfo.InvariantCulture)
                + " redelivered=" + laterListener.RedeliveredKinds().ToString(CultureInfo.InvariantCulture)
                + " deliveries=" + string.Join(",", laterListener.Snapshot()));
        c2.Shutdown();
        p2.Shutdown();

        // ---------------- S5：POP 循环把 pullRT/pullTPS 写进 307 状态表 ----------------
        // Java 的 popMessage 回调（DefaultMQPushConsumerImpl:556-563）与 pull 回调一样要把
        // IncPullRT / IncPullTPS 记进状态表，307 应答的 statusTable 就靠这两格。POP 循环漏掉
        // 它们是**静默**的：消息照弹照 ack、消费完全正常，只有运维看板上一片 0 —— 而看板上
        // "这个消费者没在拉取"和"这个消费者压根没起来"是两种完全不同的处置。
        // ⚠ 快照每 10s 采样一次、窗口取 minute 差分，所以必须**持续有流量**并跨过两个采样点，
        //   否则 pullTPS 仍是 0，那是夹具不够长，不是判据错。
        Console.WriteLine("=== S5 POP 循环把 pullRT/pullTPS 写进 307 状态表 ===");
        var statsListener = new SuccessListener();
        var c5 = new DefaultMQPushConsumer(groupStats);
        c5.SetNamesrvAddr(nsAddr);
        c5.PopMode = true;
        c5.SetConsumeThreadNums(2);
        c5.SetMessageListener(statsListener);
        c5.Subscribe(topicStats, "*");
        c5.Start();
        Thread.Sleep(1000);

        var p5 = new DefaultMQProducer("PG_PopConsNetStats_" + Stamp);
        p5.NamesrvAddr = nsAddr;
        p5.Start();
        int sent5 = 0;
        long began5 = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - began5 < 26000)   // 约 3 个采样周期
        {
            for (int i = 0; i < 4; i++)
            {
                p5.Send(new Message(topicStats,
                    Encoding.UTF8.GetBytes("s5-" + i.ToString(CultureInfo.InvariantCulture))));
                sent5++;
            }
            Thread.Sleep(2000);
        }

        var admin = new DefaultMQAdminExt("PopConsNetAdmin");
        admin.SetNamesrvAddr(nsAddr);
        admin.Start();
        // 走 broker 转发到消费者本体的 307；statusTable 是透传 JSON，按键取回后再解
        JsonValue status = JsonValue.Null;
        try
        {
            ConsumerRunningInfo ri = admin.ExamineConsumerRunningInfo(groupStats, c5.ClientId);
            status = ri.StatusTable;
        }
        catch (Exception e)
        {
            Console.WriteLine("  !! 307 failed: " + e.Message);
        }

        JsonValue cs = status.Get(topicStats);
        double pullRT = cs.Get("pullRT").DoubleValue();
        double pullTPS = cs.Get("pullTPS").DoubleValue();
        double consumeOKTPS = cs.Get("consumeOKTPS").DoubleValue();
        Console.WriteLine("  sent=" + sent5.ToString(CultureInfo.InvariantCulture)
            + " pullRT=" + pullRT.ToString("0.00", CultureInfo.InvariantCulture)
            + " pullTPS=" + pullTPS.ToString("0.0000", CultureInfo.InvariantCulture)
            + " consumeOKTPS=" + consumeOKTPS.ToString("0.0000", CultureInfo.InvariantCulture));
        Check("S5a pullRT 非 0（POP 每次 FOUND 记一次拉取耗时）", pullRT > 0.0,
            "pullRT=" + pullRT.ToString(CultureInfo.InvariantCulture));
        Check("S5b pullTPS 非 0（按弹到的条数计）", pullTPS > 0.0,
            "pullTPS=" + pullTPS.ToString(CultureInfo.InvariantCulture));
        // 拉取侧与消费侧两格各自独立：只有 consumeOKTPS 有值而 pull* 全 0，正是
        // POP 循环漏记拉取统计的特征形状。
        Check("S5c consumeOKTPS 同时非 0（两格各自独立上报）", consumeOKTPS > 0.0,
            "consumeOKTPS=" + consumeOKTPS.ToString(CultureInfo.InvariantCulture));
        // 状态表非 0 只说明"计数被调用过"，还得确认这些统计对应的流量真被消费掉：
        // 否则 pullTPS 可以靠一直接触到从未 ack 的消息刷高，看板上好看、实际在打转。
        bool consumed5 = WaitUntil(() => statsListener.Snapshot().Count >= sent5, 20000);
        Check("S5d 状态表背后的流量确实被消费了", consumed5,
            "received=" + statsListener.Snapshot().Count.ToString(CultureInfo.InvariantCulture)
                + " sent=" + sent5.ToString(CultureInfo.InvariantCulture));
        c5.Shutdown();
        p5.Shutdown();
        admin.Shutdown();

        Console.WriteLine("########################################");
        Console.WriteLine("PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
            + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture));
        return _fail == 0 ? 0 : 1;
    }
}
