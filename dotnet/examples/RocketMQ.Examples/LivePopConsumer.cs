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
//
// ⚠ S2 的观测窗口必须 > PopInvisibleTime，否则"ack 完全没发出去"也看不出重复投递
//   （消息还没到复活时间）——这是最容易伪装成通过的假绿。
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;

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
        const int nMsg = 12;

        Console.WriteLine("=".PadRight(70, '='));
        Console.WriteLine("POP consumer live (.NET): namesrv=" + nsAddr + " topic=" + topic);
        Console.WriteLine("=".PadRight(70, '='));

        var prep = new DefaultMQProducer("PG_PopConsNetPrep_" + Stamp);
        prep.NamesrvAddr = nsAddr;
        prep.Start();
        PrepareTopic(prep, topic);
        PrepareTopic(prep, topicLater);
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

        Console.WriteLine("########################################");
        Console.WriteLine("PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
            + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture));
        return _fail == 0 ? 0 : 1;
    }
}
