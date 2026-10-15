// The difference from rmq pop (the protocol pipe): what is verified here is the **consume
// loop** --
//   POP pops -> delivers to the listener -> ack on success, extend the invisible time on
//   failure -> no duplicate delivery.
// The consume loop neither queries nor commits consumer offsets; progress is tracked
// entirely by the broker's checkpoint.
//
// Scenarios:
//   S1 POP consumption: start the consumer -> send 12 messages -> all received, no
//      duplicates, the set of bodies matches
//   S2 ack takes effect: after receiving everything, observe for a while (> invisibleTime)
//      -> nothing must be redelivered
//   S3 RECONSUME_LATER: the listener keeps returning RECONSUME_LATER -> redelivered
//      following the delay tiers
//   S4 Multiple queues: the messages really landed on several queues and all of them were
//      consumed (POP pops queue by queue)
//   S5 Pull statistics: the POP path must also record pullRT/pullTPS into the 307 status
//      table (continuous traffic + real RPCs)
//
// Note: S2's observation window must be > PopInvisibleTime, otherwise "the ack never went
//   out at all" shows no duplicate delivery either (the message has not reached its revival
//   time yet) -- this is the false green most likely to slip through.
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

        // the number of distinct messages that were delivered at least twice ("every one of
        // them was redelivered" is the semantics S3 has to prove)
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
        Console.WriteLine("POP consumer live (C#): namesrv=" + nsAddr + " topic=" + topic);
        Console.WriteLine("=".PadRight(70, '='));

        var prep = new DefaultMQProducer("PG_PopConsNetPrep_" + Stamp);
        prep.NamesrvAddr = nsAddr;
        prep.Start();
        PrepareTopic(prep, topic);
        PrepareTopic(prep, topicLater);
        PrepareTopic(prep, topicStats);
        prep.Shutdown();

        // ---------------- S1 / S2 / S4: normal consumption + ack ----------------
        Console.WriteLine("=== S1 POP consumption (all received + no duplicates) ===");
        var listener = new SuccessListener();
        var consumer = new DefaultMQPushConsumer(group);
        consumer.SetNamesrvAddr(nsAddr);
        consumer.PopMode = true;
        consumer.SetConsumeThreadNums(4);
        consumer.ConsumeMessageBatchMaxSize = 4;
        // Deliberately pushed down to 10s: so that "not acked -> revived redelivery when
        // invisibleTime expires" has a chance to show up inside the observation window
        consumer.PopInvisibleTime = 10000;
        consumer.SetMessageListener(listener);
        consumer.Subscribe(topic, "*");
        // start the consumer before sending the messages (see the project convention)
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
        // must be > PopInvisibleTime(10s), otherwise a missing ack shows no duplicate delivery either
        Thread.Sleep(16000);

        List<string> got = listener.Snapshot();
        var gotSet = new HashSet<string>(got, StringComparer.Ordinal);
        Check("S1a all messages were consumed", all && got.Count >= nMsg,
            "received=" + got.Count.ToString(CultureInfo.InvariantCulture)
                + "/" + nMsg.ToString(CultureInfo.InvariantCulture));
        Check("S1b the set of bodies matches what was sent", gotSet.SetEquals(sent),
            "got=" + gotSet.Count.ToString(CultureInfo.InvariantCulture)
                + " sent=" + sent.Count.ToString(CultureInfo.InvariantCulture));
        Check("S2a no duplicate delivery", gotSet.Count == got.Count,
            "received=" + got.Count.ToString(CultureInfo.InvariantCulture)
                + " unique=" + gotSet.Count.ToString(CultureInfo.InvariantCulture));
        Check("S2b no new deliveries within the observation window", got.Count == nMsg,
            "received=" + got.Count.ToString(CultureInfo.InvariantCulture));
        Check("S4 the messages spread over several queues and all were consumed", listener.QueueCount() > 1,
            "queues=" + listener.QueueCount().ToString(CultureInfo.InvariantCulture));
        consumer.Shutdown();
        producer.Shutdown();

        // ---------------- S3: RECONSUME_LATER -> redelivery after the delay ----------------
        Console.WriteLine("=== S3 RECONSUME_LATER -> redelivery after the delay ===");
        var laterListener = new LaterListener();
        var c2 = new DefaultMQPushConsumer(groupLater);
        c2.SetNamesrvAddr(nsAddr);
        c2.PopMode = true;
        c2.SetConsumeThreadNums(2);
        c2.PopInvisibleTime = 5000;
        // the delay tier on consumption failure: push the first tier down to 3s so that
        // "extend the invisible time -> visible again" happens as soon as possible
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
        Check("S3a the first round delivered 3", first && firstRound >= 3,
            "count=" + firstRound.ToString(CultureInfo.InvariantCulture));

        // Note: what is asserted is "every one of them was redelivered", not "3 more
        // deliveries arrived": counting by message, one message redelivered 3 times while
        // another never came back would still pass.
        bool again = WaitUntil(() => laterListener.RedeliveredKinds() >= 3, 40000);
        Check("S3b every message that failed consumption was redelivered (extending the invisible time took effect)", again,
            "first=" + firstRound.ToString(CultureInfo.InvariantCulture)
                + " redelivered=" + laterListener.RedeliveredKinds().ToString(CultureInfo.InvariantCulture)
                + " deliveries=" + string.Join(",", laterListener.Snapshot()));
        c2.Shutdown();
        p2.Shutdown();

        Console.WriteLine("=== S5 the POP loop writes pullRT/pullTPS into the 307 status table ===");
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
        while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - began5 < 26000)   // about 3 sampling periods
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
        // goes through the broker forwarded to the consumer itself as 307; statusTable is
        // passed-through JSON, so it is fetched by key and then decoded
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
        Check("S5a pullRT is non-zero (POP records a pull duration on every FOUND)", pullRT > 0.0,
            "pullRT=" + pullRT.ToString(CultureInfo.InvariantCulture));
        Check("S5b pullTPS is non-zero (counted by the number of messages popped)", pullTPS > 0.0,
            "pullTPS=" + pullTPS.ToString(CultureInfo.InvariantCulture));
        // the pull side and the consume side are independent slots: only consumeOKTPS having
        // a value while pull* are all 0 is exactly the shape of a POP loop that forgot to
        // record the pull statistics.
        Check("S5c consumeOKTPS is non-zero at the same time (the two slots report independently)", consumeOKTPS > 0.0,
            "consumeOKTPS=" + consumeOKTPS.ToString(CultureInfo.InvariantCulture));
        // A non-zero status table only means "the counter was called"; it still has to be
        // confirmed that the traffic behind those statistics really was consumed: otherwise
        // pullTPS can be pushed up by continuously touching messages that were never acked,
        // which looks good on the dashboard while it is actually spinning.
        bool consumed5 = WaitUntil(() => statsListener.Snapshot().Count >= sent5, 20000);
        Check("S5d the traffic behind the status table really was consumed", consumed5,
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
