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

    /// <summary>Polls until the condition holds and returns whether it held within the
    /// window.
    /// Real-machine assertions use this instead of a fixed sleep: when several live suites
    /// run concurrently the broker gets slower, and a fixed sleep measures a false failure
    /// (the same code turns green on a re-run).</summary>
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

    /// <summary>The queue key (identical to the consumer's internal OffsetKey): topic +
    /// brokerName + queueId.</summary>
    private static string Key(MessageQueue mq) =>
        mq.Topic + mq.BrokerName + mq.QueueId.ToString(CultureInfo.InvariantCulture);

    /// <summary>Creates the topic first the way real usage does, then starts the
    /// consumer.
    /// In a real environment the topic is pre-created by an administrator or by the first
    /// send; the consumer does no default-topic fallback, so when the topic does not exist
    /// it gets no route and is assigned no queues.</summary>
    private static void PrepareTopic(DefaultMQProducer producer, string topic, int queues = 4)
    {
        try
        {
            producer.CreateTopic("init", topic, queues);
        }
        catch (Exception e)
        {
            Console.WriteLine("  pre-creating topic " + topic + " failed (falling back to auto-creation): " + e.Message);
        }

        // Wait for the NameServer route to propagate, otherwise the consumer's first rebalance round still finds nothing
        Thread.Sleep(3000);
    }

    // ---------------- S1 Resend ----------------
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

            // retry-me fails on the first delivery and succeeds after the resend (the resend count goes through field 13 of MessageExt)
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
        Console.WriteLine("S1: sent, waiting for the resend (delay tier level3~10s)...");
        Thread.Sleep(22000);
        consumer.Shutdown();

        List<(string Body, string Topic, int ReconsumeTimes, long Ts)> retryArrivals;
        int normalCount;
        var snap = listener.Snapshot();
        retryArrivals = snap.Where(a => a.Body == "retry-me").ToList();
        normalCount = snap.Count(a => a.Body == "normal-1");

        Check("S1-retry-me was delivered more than once", retryArrivals.Count >= 2,
            "arrivals=" + retryArrivals.Count.ToString(CultureInfo.InvariantCulture));
        bool fromRetry = retryArrivals.Any(a => a.ReconsumeTimes >= 1 || a.Topic.StartsWith("%RETRY%", StringComparison.Ordinal));
        Check("S1-the resend came from %RETRY%/reconsumeTimes", fromRetry);
        if (retryArrivals.Count >= 2)
        {
            double gapSec = (retryArrivals[^1].Ts - retryArrivals[0].Ts) / 1000.0;
            Check("S1-the resend has a delay tier (>=8s)", gapSec >= 8.0,
                "gap=" + gapSec.ToString("F1", CultureInfo.InvariantCulture) + "s");
        }
        else
        {
            Check("S1-the resend has a delay tier (>=8s)", false, "fewer than two deliveries");
        }

        Check("S1-a normal message is delivered only once", normalCount == 1,
            "arrivals=" + normalCount.ToString(CultureInfo.InvariantCulture));
    }

    // ---------------- S2 Offset persistence ----------------
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
        c1.Shutdown();  // shutdown persists the offsets
        round1 = l1.Snapshot();

        int gotFirst = Enumerable.Range(0, 3).Count(i => round1.Contains("persist-" + i.ToString(CultureInfo.InvariantCulture)));
        Check("S2-the first round consumed 3", gotFirst == 3, "got=" + gotFirst.ToString(CultureInfo.InvariantCulture));

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
        Check("S2-after the restart new messages keep being delivered", newSeen, newSeen ? "" : "nothing received");
        Check("S2-the restart does not re-consume old messages", oldResent == 0,
            "duplicates=" + oldResent.ToString(CultureInfo.InvariantCulture));
    }

    // ---------------- S3 Orderly consumption + broker locks ----------------
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
        Thread.Sleep(5000);  // wait for the first LOCK_BATCH_MQ round to take effect
        for (int i = 0; i < 4; ++i)
        {
            producer.Send(new Message(topic, Str2Bytes("orderly-" + i.ToString(CultureInfo.InvariantCulture))));
        }

        Thread.Sleep(8000);
        consumer.Shutdown();
        Check("S3-orderly consumption received everything", listener.Got == 4,
            "got=" + listener.Got.ToString(CultureInfo.InvariantCulture));
        Check("S3-the orderly consumption path is alive", listener.Got > 0);
    }

    // ---------------- S4 Broadcasting mode ----------------
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
        Check("S4-broadcasting consumer A received everything", la.Snapshot().Count == 3,
            "got=" + la.Snapshot().Count.ToString(CultureInfo.InvariantCulture));
        Check("S4-broadcasting consumer B received everything", lb.Snapshot().Count == 3,
            "got=" + lb.Snapshot().Count.ToString(CultureInfo.InvariantCulture));
    }

    // ---------------- S5 Flow control ----------------
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

        // A threshold of 2 plus 300ms of sleep per message: only everything landing proves
        // that "flow control just pauses pulling, it does not drop messages". A fixed sleep
        // on a busy machine measures a false failure of got=7/10 (the same code turns green
        // on a re-run).
        bool allArrived = WaitUntil(() => listener.Got >= 10, 45000);
        long fc = consumer.FlowControlTriggered;
        consumer.Shutdown();
        Check("S5-all messages arrive under slow consumption", allArrived && listener.Got == 10,
            "got=" + listener.Got.ToString(CultureInfo.InvariantCulture));
        Check("S5-the flow control trigger count is >0", fc > 0,
            "triggered=" + fc.ToString(CultureInfo.InvariantCulture));
    }

    // ---------------- S6 Cluster multi-instance rebalance (queue assignment) ----------------
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

        // all the queues of the main topic (expected to be fully covered by the two instances)
        var expectedKeys = new HashSet<string>(StringComparer.Ordinal);
        foreach (MessageQueue mq in ca.FetchSubscribeMessageQueues(topic))
        {
            expectedKeys.Add(Key(mq));
        }

        // wait for the assignment to settle: empty intersection + both non-empty + the main topic's queues fully covered (at most 45s)
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
        // the broker reverse-pushes 40 over the long connection when the group membership changes; a reverse request case cannot inject it, so the
        // counter is the only landing point that can prove "the instance-level 40 handler really ran" (it must be sampled before Shutdown).
        long notifiedA = ca.Client().ConsumerIdsChangedCount;
        long notifiedB = cb.Client().ConsumerIdsChangedCount;
        ca.Shutdown();
        cb.Shutdown();

        Check("S6-the consumer registered via heartbeat", hbA > 0 && cidList.Count == 2,
            "heartbeats=" + hbA.ToString(CultureInfo.InvariantCulture)
                + " brokerCids=" + cidList.Count.ToString(CultureInfo.InvariantCulture));

        var sa = new HashSet<string>(keysA, StringComparer.Ordinal);
        var sb = new HashSet<string>(keysB, StringComparer.Ordinal);
        int cross = sa.Intersect(sb).Count();
        var cover = new HashSet<string>(sa, StringComparer.Ordinal);
        cover.UnionWith(sb);
        cover.IntersectWith(expectedKeys);
        Check("S6-no queue missing and none duplicated (a=" + keysA.Count.ToString(CultureInfo.InvariantCulture)
              + ",b=" + keysB.Count.ToString(CultureInfo.InvariantCulture)
              + ",intersection=" + cross.ToString(CultureInfo.InvariantCulture)
              + ",covered=" + cover.Count.ToString(CultureInfo.InvariantCulture)
              + "/" + expectedKeys.Count.ToString(CultureInfo.InvariantCulture) + ")",
            keysA.Count > 0 && keysB.Count > 0 && cross == 0 && expectedKeys.Count > 0
                && cover.Count == expectedKeys.Count);
        Check("S6-no message consumed twice", dup == 0 && all.Count == total,
            "got=" + all.Count.ToString(CultureInfo.InvariantCulture)
                + "/" + total.ToString(CultureInfo.InvariantCulture)
                + " dup=" + dup.ToString(CultureInfo.InvariantCulture));
        Check("S6-on a membership change the broker's NOTIFY_CONSUMER_IDS_CHANGED(40) is received",
            notifiedA > 0 && notifiedB > 0,
            "a=" + notifiedA.ToString(CultureInfo.InvariantCulture)
                + " b=" + notifiedB.ToString(CultureInfo.InvariantCulture)
                + " (0 means the instance-level handler never received a reverse notification)");
    }

    // ---------------- S7 Graceful unregister ----------------
    private static void ScenarioUnregister()
    {
        // Reuses the 8-queue topic built in S6; shutdown should send UNREGISTER_CLIENT and
        // the broker removes it immediately, with no need to wait for the heartbeat timeout
        // (~120s). The query uses an independent probe client (after the consumer shuts down
        // its internal client is already closed).
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

        Check("S7-shutdown unregistered the clientId",
            before.Contains(cid) && !after.Contains(cid),
            "before=" + before.Count.ToString(CultureInfo.InvariantCulture)
                + " after=" + after.Count.ToString(CultureInfo.InvariantCulture));
    }

    // ---------------- S8 Namespace (multi-tenant isolation)----------------
    private static void ScenarioNamespace()
    {
        string ns = "NSDotnet" + (NowMs() % 100000).ToString(CultureInfo.InvariantCulture);
        string topic = _gPrefix + "_Ns";

        var nsProducer = new DefaultMQProducer(_gPrefix + "_ns_pg");
        nsProducer.NamesrvAddr = _namesrv;
        nsProducer.Namespace = ns;
        nsProducer.Start();
        // CreateTopic also goes through the namespace wrapping: what really gets created is "<ns>%<topic>"
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
        Check("S8-production/consumption with a namespace received everything", nsGot.Count == 3,
            "got=" + nsGot.Count.ToString(CultureInfo.InvariantCulture));

        // a consumer without a namespace subscribing to the same bare topic -> receives nothing (proving the real topic is ns%topic)
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
            Console.WriteLine("  (the no-ns consumer threw, as isolation requires: " + e.Message + ")");
        }

        List<string> plainGot = plainListener.Snapshot();
        Check("S8-a consumer without a namespace receives nothing (isolation)", plainGot.Count == 0,
            "got=" + plainGot.Count.ToString(CultureInfo.InvariantCulture));

        nsProducer.Shutdown();
    }

    // ---------------- S9 Dead letter terminal state (%DLQ%) ----------------
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

        // resend tier = 3 + reconsumeTimes => level3(10s) + level4(30s), plus some delivery slack.
        // 150s rather than 100s: when the whole machine runs the other suites concurrently
        // the broker's timer service delays the tiers, and the third delivery has been
        // measured arriving 60s+ late, so cutting at 100s is a false failure.
        long deadline = NowMs() + 150000;
        while (NowMs() < deadline && listener.Snapshot().Count < 3) Thread.Sleep(2000);
        List<(string Body, string Topic, int ReconsumeTimes)> firstThree =
            listener.Snapshot().Where(a => a.Body == "dlq-me").ToList();
        Thread.Sleep(15000);  // counter-proof: there must be no 4th delivery
        List<(string Body, string Topic, int ReconsumeTimes)> finalSeen =
            listener.Snapshot().Where(a => a.Body == "dlq-me").ToList();
        consumer.Shutdown();

        string times = string.Join(",", firstThree.Select(a => a.ReconsumeTimes
            .ToString(CultureInfo.InvariantCulture)));
        Check("S9-maxReconsumeTimes=2 => delivered 3 times (reconsumeTimes 0/1/2)",
            firstThree.Count >= 3 && firstThree[0].ReconsumeTimes == 0
                && firstThree[1].ReconsumeTimes == 1 && firstThree[2].ReconsumeTimes == 2,
            "times=[" + times + "]");
        Check("S9-after it is used up there are no more deliveries (only 3 within the observation window)", finalSeen.Count == 3,
            "arrivals=" + finalSeen.Count.ToString(CultureInfo.InvariantCulture));
        Check("S9-during the resend the listener sees the business topic (not %RETRY%)",
            firstThree.All(a => a.Topic == topic));

        // %DLQ%<group> is only created by the broker at the moment it moves the dead letter and registers it with the namesrv
        string dlqTopic = MixAll.GetDlqTopic(group);
        (bool routeFound, List<MessageExt> dlqMsgs) = ReadDlq(group, _gPrefix + "_g9dlq", 30000);
        Check("S9-the broker auto-created and registered the %DLQ%<group> route", routeFound, "dlq=" + dlqTopic);

        bool one = dlqMsgs.Count == 1 && Body(dlqMsgs[0]) == "dlq-me";
        Check("S9-the message landed in %DLQ%<group>", one,
            "n=" + dlqMsgs.Count.ToString(CultureInfo.InvariantCulture));
        if (one)
        {
            MessageExt d = dlqMsgs[0];
            Check("S9-the dead letter's reconsumeTimes = maxReconsumeTimes + 1 (+1 as the broker stores it)",
                d.ReconsumeTimes == maxReconsume + 1,
                "reconsumeTimes=" + d.ReconsumeTimes.ToString(CultureInfo.InvariantCulture));
            bool retryKept = d.Properties.TryGetValue("RETRY_TOPIC", out string? origin)
                && origin == topic && d.Topic == dlqTopic;
            Check("S9-the dead letter keeps RETRY_TOPIC=the business topic while the topic is already %DLQ%<group>", retryKept,
                "retryTopic=" + (d.Properties.TryGetValue("RETRY_TOPIC", out string? o)
                    ? o : "<missing>"));
        }
    }

    // ---------------- S10 Partial ack (ackIndex) ----------------
    /// <summary>Records every delivery and (optionally) narrows ctx.AckIndex on the
    /// **first batch only**.</summary>
    private sealed class PartialAckListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<(string Body, string Topic, int ReconsumeTimes)> _seen = new();
        private readonly List<int> _batchSizes = new();

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
                // Only the first batch is narrowed: later batches must acknowledge the whole
                // batch, otherwise that tail is never finished being resent and the
                // "the acknowledged prefix is delivered once" counter is polluted by the
                // later batches.
                narrow = _ackFirst >= 0 && _batchSizes.Count == 1;
            }

            if (narrow) ctx.AckIndex = _ackFirst;
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    private static void ScenarioPartialAck(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_AckIndex";
        // 1 queue: only a batch of 3 is contiguous and ordered, so the tail is a real tail
        PrepareTopic(producer, topic, 1);
        string group = _gPrefix + "_g10";
        string controlGroup = _gPrefix + "_g10ctrl";

        var partial = new PartialAckListener(0);
        var control = new PartialAckListener(-1);
        var pc = NewConsumer(group);
        var cc = NewConsumer(controlGroup);
        // the default batch is 1 message, so without narrowing to 3 there is no "partial" at all
        pc.ConsumeMessageBatchMaxSize = 3;
        cc.ConsumeMessageBatchMaxSize = 3;
        pc.SetMessageListener(partial);
        cc.SetMessageListener(control);
        pc.Subscribe(topic, "*");
        cc.Subscribe(topic, "*");
        // Put the 3 messages in place before starting the consumer: how a batch is cut is
        // decided by the pull timing, and only when the queue already holds the 3 messages
        // is the very first pull exactly "one whole batch of 3", otherwise the first batch
        // could be 1~2 messages and the prefix/suffix cut by ackIndex=0 would be
        // undetermined. A new group under LAST_OFFSET starts from the newest offset at
        // assignment time, hence the explicit consumption from 0.
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

        // the tail has to go through %RETRY% (delay level 3~10s) + route registration of the retry topic + the next rebalance round
        bool tailBack = WaitUntil(() => Redelivered(partial) == tail.Length, 150000);
        Thread.Sleep(10000);  // counter-proof window: a redundant duplicate delivery shows up here
        List<(string Body, string Topic, int ReconsumeTimes)> seenFinal = partial.Snapshot();

        Check("S10-the first batch really got 3 messages (only ackIndex=0 makes a 'partial' meaningful)",
            partial.FirstBatchSize == 3,
            "firstBatch=" + partial.FirstBatchSize.ToString(CultureInfo.InvariantCulture));
        Check("S10-the unacknowledged tail comes back from %RETRY% (reconsumeTimes>=1 and the topic is the business topic)",
            tailBack,
            "redelivered=" + Redelivered(partial).ToString(CultureInfo.InvariantCulture) + "/2");
        int ackedHits = seenFinal.Count(r => r.Body == "ack-0");
        Check("S10-the acknowledged one is delivered only once in the whole window (the prefix was not resent either)", ackedHits == 1,
            "arrivals=" + ackedHits.ToString(CultureInfo.InvariantCulture));
        Check("S10-all 3 are eventually consumed (nothing is lost)",
            seenFinal.Select(r => r.Body).Distinct().Count() == 3,
            "distinct=" + seenFinal.Select(r => r.Body).Distinct().Count()
                .ToString(CultureInfo.InvariantCulture));

        List<(string Body, string Topic, int ReconsumeTimes)> ctrl = control.Snapshot();
        Check("S10-the control group's default ackIndex(MAX_VALUE): nothing is resent",
            ctrl.Count == 3 && ctrl.All(r => r.ReconsumeTimes == 0),
            "deliveries=" + ctrl.Count.ToString(CultureInfo.InvariantCulture)
            + " retried=" + ctrl.Count(r => r.ReconsumeTimes >= 1).ToString(CultureInfo.InvariantCulture));

        // The broker's rule: the business queue offsets of both groups must be committed as a whole to 3 (a partial ack is not
        // "committing less", the tail has been handed to the broker for resending so this queue owes nothing)
        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_namesrv);
        admin.SetTimeoutMillis(10000);
        try
        {
            admin.Start();
            List<MessageQueue> queues = pc.FetchSubscribeMessageQueues(topic);
            Check("S10-the business topic has a queue whose offset can be queried", queues.Count > 0,
                "queues=" + queues.Count.ToString(CultureInfo.InvariantCulture));
            if (queues.Count > 0)
            {
                MessageQueue mq = queues[0];
                // -1 = the broker has no offset for that group yet (QUERY_NOT_FOUND)
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
                Check("S10-after a partial ack the business queue's offset still advances as a whole to 3", p,
                    "committed=" + ReadOffset(group).ToString(CultureInfo.InvariantCulture));
                bool c = WaitUntil(() => ReadOffset(controlGroup) == 3, 30000);
                Check("S10-the control group's business queue offset also reaches 3", c,
                    "committed=" + ReadOffset(controlGroup).ToString(CultureInfo.InvariantCulture));
            }
        }
        catch (Exception e)
        {
            Check("S10-the offset query is usable", false, e.Message);
        }

        admin.Shutdown();
        pc.Shutdown();
        cc.Shutdown();
    }


    private static void ScenarioPullStallSelfHeal(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_Heal";
        string group = _gPrefix + "_g11";
        // 1 queue: only one loop exists, the injection point is unique and so is the offset criterion (multiple queues get shared out, and the one withdrawn is not necessarily the injected one)
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
            Check("S11-the single queue is assigned to this instance", queues.Count == 1 && c.AssignedQueueKeys().Contains(Key(queues[0])),
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

            // ---- H1 baseline: 3 consumed, the offset committed to 3, the pull clock is real ----
            for (int i = 0; i < 3; ++i)
            {
                producer.Send(new Message(topic, Str2Bytes("heal-1-" + i.ToString(CultureInfo.InvariantCulture))));
            }

            Check("S11-H1 baseline: 3 were consumed",
                WaitUntil(() => listener.Snapshot().Count >= 3, 30000),
                "arrivals=" + listener.Snapshot().Count.ToString(CultureInfo.InvariantCulture));
            Check("S11-H1 baseline: the offset is committed to the broker (the withdraw/rebuild resume relies on it)",
                WaitUntil(() => Committed() == 3, 30000),
                "committed=" + Committed().ToString(CultureInfo.InvariantCulture));

            long stamp = c.LastPullAt(key);
            Check("S11-H1 the loop stamps when it starts a pull", stamp > 0 && NowMs() - stamp < 60000,
                "lastPullAt=" + stamp.ToString(CultureInfo.InvariantCulture));
            Check("S11-H1 the in-process consumed offset is 3 (self-healing has to carry it to persistence)",
                c.ConsumeOffsetForTest(key) == 3,
                "consumeOffset=" + (c.ConsumeOffsetForTest(key) ?? -1).ToString(CultureInfo.InvariantCulture));

            // ---- H2 the loop is broken through by an exception -> the same rebalance round withdraws and rebuilds it ----
            c.RegisterLoopForTest(key, alive: false);
            Check("S11-H2 the thread has exited so it is judged stalled at once (no need to wait out the full 120s)", c.PullStalled(key), "key=" + key);
            c.WakeupRebalanceForTest();
            // Only "the registered entry was replaced by a genuinely live thread" makes PullStalled go false: the injected
            // placeholder thread is never alive, so this assertion is equivalent to "it really was withdrawn and rebuilt".
            Check("S11-H2 the stalled queue was withdrawn and rebuilt (a new loop took over and stamped again)",
                WaitUntil(() => !c.PullStalled(key), 30000),
                "lastPullAt=" + c.LastPullAt(key).ToString(CultureInfo.InvariantCulture));
            Check("S11-H2 the rebuild did not write the offset back (no rollback)", Committed() >= 3,
                "committed=" + Committed().ToString(CultureInfo.InvariantCulture));

            for (int i = 0; i < 3; ++i)
            {
                producer.Send(new Message(topic, Str2Bytes("heal-2-" + i.ToString(CultureInfo.InvariantCulture))));
            }

            Check("S11-H2 after self-healing the same queue keeps being consumed (6 in total)",
                WaitUntil(() => listener.Snapshot().Count >= 6, 30000) && WaitUntil(() => Committed() == 6, 30000),
                "arrivals=" + listener.Snapshot().Count.ToString(CultureInfo.InvariantCulture)
                + " committed=" + Committed().ToString(CultureInfo.InvariantCulture));

            // ---- H3 the stamp goes past 120s (the thread is still alive)----
            long injected = NowMs() - DefaultMQPushConsumer.PullMaxIdleTimeMillis - 5000;
            string want = "\"lastPullTimestamp\":" + injected.ToString(CultureInfo.InvariantCulture);
            bool reported = false;
            bool expired = false;
            // Re-set rather than set once: a real loop refreshes this instant itself on every
            // round, so hitting the previous one is very unlikely but not impossible (a long
            // poll returning right in those milliseconds), and setting it twice lets both the
            // criterion and the message be observed at once.
            for (int attempt = 0; attempt < 5 && !(reported && expired); ++attempt)
            {
                c.SetLastPullAt(key, injected);
                expired = c.PullStalled(key);
                reported = Encoding.UTF8.GetString(c.BuildConsumerRunningInfo().Encode()).Contains(want);
            }

            Check("S11-H3 rewinding past 120s is judged stalled", expired, "injected=" + injected);
            Check("S11-H3 the running info reports lastPullTimestamp as the stamped value", reported, "want=" + want);
            c.WakeupRebalanceForTest();
            Check("S11-H3 the stalled branch was handled (this path re-stamped or a new loop was put in place)",
                WaitUntil(() => c.LastPullAt(key) > injected, 30000),
                "lastPullAt=" + c.LastPullAt(key).ToString(CultureInfo.InvariantCulture));

            for (int i = 0; i < 3; ++i)
            {
                producer.Send(new Message(topic, Str2Bytes("heal-3-" + i.ToString(CultureInfo.InvariantCulture))));
            }

            Check("S11-H3 consumption still works as usual afterwards (9 in total, the offset at 9)",
                WaitUntil(() => listener.Snapshot().Count >= 9, 30000) && WaitUntil(() => Committed() == 9, 30000),
                "arrivals=" + listener.Snapshot().Count.ToString(CultureInfo.InvariantCulture)
                + " committed=" + Committed().ToString(CultureInfo.InvariantCulture));

            // Leave a window for errors like "a rebuild rolls the offset back with a stale cursor": a wholesale resend of the queue shows up here
            Thread.Sleep(8000);
            List<(string Body, string Topic, int ReconsumeTimes, long Ts)> seen = listener.Snapshot();
            int distinct = seen.Select(r => r.Body).Distinct().Count();
            int redelivered = seen.Count(r => r.ReconsumeTimes != 0);
            Check("S11-each of the 9 is delivered exactly once (the offset was persisted before the withdrawal and the rebuild resumes from the broker's offset)",
                distinct == 9 && redelivered == 0 && seen.Count == 9,
                "arrivals=" + seen.Count.ToString(CultureInfo.InvariantCulture)
                + " distinct=" + distinct.ToString(CultureInfo.InvariantCulture)
                + " redelivered=" + redelivered.ToString(CultureInfo.InvariantCulture));
            Check("S11-after self-healing the pull clock is fresh again (the criterion stops complaining)",
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

    // ---------------- S12 Orderly-consumption poison message: local counting -> handing it to the broker -> %DLQ% ----------------

    /// <summary>Suspends the current queue on every orderly delivery matching the given
    /// body (everything else succeeds as usual).</summary>
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

    private static void ScenarioOrderlyDlq(DefaultMQProducer producer)
    {
        const int maxReconsume = 2;
        const string poison = "ord-poison";
        string topic = _gPrefix + "_OrdDlq";
        string group = _gPrefix + "_g12";
        // 1 queue + 1 message per batch: ord-after must be ordered behind the poison message, and suspending it does not drag anything else in
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

        Check("S12-the poison message is delivered exactly 3 times (each time the client itself does +1: reconsumeTimes 0/1/2)",
            WaitUntil(() => listener.CountOf(poison) >= 3, 60000)
            && LadderOf(listener, poison),
            "times=[" + TimesOf(listener, poison) + "]");
        Check("S12-after handing it to the broker the business queue keeps advancing (the next message is consumed)",
            WaitUntil(() => listener.CountOf("ord-after") >= 1, 60000),
            "after=" + listener.CountOf("ord-after").ToString(CultureInfo.InvariantCulture));
        Thread.Sleep(15000);  // counter-proof: there must be no 4th time
        Check("S12-after it is used up it is no longer suspended in place (the poison message was only delivered 3 times in the window)",
            listener.CountOf(poison) == 3,
            "arrivals=" + listener.CountOf(poison).ToString(CultureInfo.InvariantCulture));
        Check("S12-during the suspension the listener always sees the business topic (a local resend does not change the topic)",
            listener.Snapshot().Where(a => a.Body == poison).All(a => a.Topic == topic));
        consumer.Shutdown();

        string dlqTopic = MixAll.GetDlqTopic(group);
        (bool routeFound, List<MessageExt> dlqMsgs) = ReadDlq(group, _gPrefix + "_g12dlq", 40000);
        Check("S12-the broker auto-created and registered the %DLQ%<group> route", routeFound, "dlq=" + dlqTopic);
        // An orderly resend landing in the dead letter instead of going back to %RETRY% is
        // itself the broker deciding "this group's rebalance lock has not expired yet" =>
        // this instance really holds LOCK_BATCH_MQ (handleRetryAndDLQ:202-207).
        bool one = dlqMsgs.Count == 1 && Body(dlqMsgs[0]) == poison;
        Check("S12-the poison message landed in %DLQ%<group> (the resend goes through the rebalance lock and enters the dead letter at once)",
            one, "n=" + dlqMsgs.Count.ToString(CultureInfo.InvariantCulture));
        if (one)
        {
            MessageExt d = dlqMsgs[0];
            // 3 = the +1 the client wrote on RECONSUME_TIME, which becomes the stored value
            // through the V2 header's j; if j were left out the broker would judge by the
            // subscription group's default of 16 and this message would never reach the dead letter.
            Check("S12-the dead letter's reconsumeTimes = maxReconsumeTimes + 1",
                d.ReconsumeTimes == maxReconsume + 1,
                "reconsumeTimes=" + d.ReconsumeTimes.ToString(CultureInfo.InvariantCulture));
            bool hasRetryTopic = d.Properties.TryGetValue("RETRY_TOPIC", out string? retryTopic);
            Check("S12-the dead letter keeps RETRY_TOPIC=the business topic while the topic is already %DLQ%<group>",
                hasRetryTopic && retryTopic == topic && d.Topic == dlqTopic,
                "retryTopic=" + (hasRetryTopic ? retryTopic : "<missing>"));
        }
    }

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
        // Explicitly setting no cap (the default is -1; writing it out gives the "default" assertion a source)
        consumer.MaxReconsumeTimes = -1;
        consumer.SetMessageListener(listener);
        consumer.Subscribe(topic, "*");
        consumer.Start();
        Thread.Sleep(3000);
        producer.Send(new Message(topic, Str2Bytes(poison)));

        // Going past the concurrency side's 16 is enough to prove the wrong constant was not used. The window is
        // 90s: if it were wrong the 17th delivery's delay tier would be level20 (2h), and
        // once it goes out it never comes back, so it has to be pinned from both ends, "how
        // many times the client delivered locally" plus "an empty %DLQ%".
        Check("S12b-with maxReconsumeTimes=-1 orderly consumption sets no cap (delivered >=18 times, the 16 does not apply)",
            WaitUntil(() => listener.CountOf(poison) >= 18, 90000)
            && listener.Snapshot().Where(a => a.Body == poison).Max(a => a.ReconsumeTimes) >= 17,
            "arrivals=" + listener.CountOf(poison).ToString(CultureInfo.InvariantCulture)
            + " times=[" + TimesOf(listener, poison) + "]");
        consumer.Shutdown();

        (bool routeFound, List<MessageExt> dlqMsgs) = ReadDlq(group, _gPrefix + "_g12bdlq", 20000);
        Check("S12b-before the threshold is reached there must be no dead letter (the broker need not even create the %DLQ% topic)",
            dlqMsgs.Count == 0,
            "routeFound=" + (routeFound ? "true" : "false")
            + " n=" + dlqMsgs.Count.ToString(CultureInfo.InvariantCulture));
    }

    /// <summary>On hitting the poison message, sets the suspend duration on the context to
    /// the given value and records this delivery's instant.</summary>
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
            // -1 by default; only an explicit assignment takes the context branch (-1 means falling back to the consumer config)
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

    private static void ScenarioOrderlySuspendMillis(DefaultMQProducer producer)
    {
        SuspendProbe(producer, "OrdSusp", "g12c", configMs: 900, askedMs: 70, poison: "ord-susp-poison",
            need: 9, minGaps: 6, timeoutMs: 25000, lo: 0.03, hi: 0.4,
            name: "S12c-the context's 70ms takes effect (not the consumer config's 900ms)");
        SuspendProbe(producer, "OrdSuspCfg", "g12ccfg", configMs: 400, askedMs: -1, poison: "ord-susp-cfg",
            need: 6, minGaps: 4, timeoutMs: 25000, lo: 0.25, hi: 0.75,
            name: "S12c-when the context stays at -1 it falls back to the consumer config's 400ms");
        SuspendProbe(producer, "OrdSuspFloor", "g12cfloor", configMs: 0, askedMs: 1, poison: "ord-susp-floor",
            need: 9, minGaps: 6, timeoutMs: 20000, lo: 0.009, hi: 10.0,
            name: "S12c-clamped lower bound: with context 1ms / config 0 there is no busy wait (interval >= 10ms)");
    }

    private static void SuspendProbe(DefaultMQProducer producer, string suffix, string groupSuffix,
        int configMs, int askedMs, string poison, int need, int minGaps, int timeoutMs, double lo,
        double hi, string name)
    {
        string topic = _gPrefix + "_" + suffix;
        string group = _gPrefix + "_" + groupSuffix;
        PrepareTopic(producer, topic, 1);  // 1 queue: only an orderly resend on the same queue gives comparable intervals

        var listener = new SuspendTimingOrderlyListener(poison, askedMs);
        var consumer = NewConsumer(group);
        consumer.ConsumeMessageBatchMaxSize = 1;
        consumer.MaxReconsumeTimes = -1;  // no cap on the orderly side, otherwise enough intervals cannot be collected
        consumer.SuspendCurrentQueueTimeMillis = configMs;
        consumer.SetMessageListener(listener);
        consumer.Subscribe(topic, "*");
        consumer.Start();
        // Wait for the first LOCK_BATCH_MQ round: without the queue lock the broker turns an orderly resend straight into the dead letter and the intervals are gone
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

    // ---------------- S13 Explicit batch ack (COMMIT) / explicit rollback (ROLLBACK) on the orderly side ----------------

    private enum ManualMode
    {
        CommitOnce,          // autoCommit=false + COMMIT: explicit whole-batch acknowledgement
        RollbackThenCommit,  // autoCommit=false + ROLLBACK: roll back to a few local resends, then acknowledge the whole batch
        IllegalRollback,     // autoCommit stays true but ROLLBACK is returned: illegal usage, treated as an ack
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

            // illegal usage: AutoCommit is not touched (stays at the default true), only the return value changes
            return ConsumeOrderlyStatus.Rollback;
        }
    }

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

    private static void ManualAckCommitProbe(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_OrdCommit";
        string group = _gPrefix + "_g13a";
        PrepareTopic(producer, topic, 1);

        var listener = new ManualOrderlyListener(ManualMode.CommitOnce, "cm-none", 0);
        var c = NewConsumer(group);
        c.ConsumeMessageBatchMaxSize = 3;
        // A new group under LAST_OFFSET counts from the assignment instant, so put 3 messages
        // in place before starting the consumer and consume explicitly from 0, only then is
        // the very first pull exactly "one whole batch of 3" (the same rule as S10)
        c.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
        c.SetMessageListener(listener);
        c.Subscribe(topic, "*");
        for (int i = 0; i < 3; ++i)
        {
            producer.Send(new Message(topic, Str2Bytes("cm-" + i.ToString(CultureInfo.InvariantCulture))));
        }

        c.Start();
        WaitUntil(() => listener.Batches().Count >= 1, 30000);
        Thread.Sleep(6000);  // counter-proof window: if COMMIT were treated as "do not commit", the 200ms suspend cadence would resend ~30 times in 6s
        List<List<string>> batches = listener.Batches();
        bool firstBatch3 = batches.Count > 0 && batches[0].Count == 3;
        Check("S13a-the first batch is the whole batch of 3 (one COMMIT acknowledges the whole batch)",
            firstBatch3,
            "firstBatch=" + (batches.Count > 0 ? batches[0].Count.ToString(CultureInfo.InvariantCulture) : "none"));
        Check("S13a-only this batch was delivered in those 6 seconds (COMMIT is not treated as SUCCESS's do-not-commit)",
            batches.Count == 1,
            "batches=" + batches.Count.ToString(CultureInfo.InvariantCulture));
        Check("S13a-each of the 3 is delivered exactly once (the whole-batch ack was not resent)",
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
                Check("S13a-the broker's offset advances in one go to the batch tail 3 (whole-batch ack, not stuck at 0)",
                    ReadCommitted(admin, mq, group) == 3,
                    "committed=" + ReadCommitted(admin, mq, group).ToString(CultureInfo.InvariantCulture));
            }
            else
            {
                Check("S13a-the business topic has a queue whose offset can be queried", false, "queues=0");
            }
        }
        catch (Exception e)
        {
            Check("S13a-the offset query is usable", false, e.Message);
        }

        admin.Shutdown();
    }

    private static void ManualRollbackProbe(DefaultMQProducer producer)
    {
        string topic = _gPrefix + "_OrdRollback";
        string group = _gPrefix + "_g13b";
        PrepareTopic(producer, topic, 1);

        var listener = new ManualOrderlyListener(ManualMode.RollbackThenCommit, "rb-head", 6);
        var c = NewConsumer(group);
        c.ConsumeMessageBatchMaxSize = 1;
        c.MaxReconsumeTimes = -1;  // no cap on the orderly side, so a local resend is not judged to be a poison message
        c.SuspendCurrentQueueTimeMillis = 200;
        c.SetMessageListener(listener);
        c.Subscribe(topic, "*");
        c.Start();
        // Wait for the first LOCK_BATCH_MQ round: without the queue lock the broker turns an orderly resend straight into the dead letter
        Thread.Sleep(4000);
        producer.Send(new Message(topic, Str2Bytes("rb-head")));
        producer.Send(new Message(topic, Str2Bytes("rb-next")));

        bool arrived = WaitUntil(() => listener.Count("rb-head") >= 7 && listener.FirstIndex("rb-next") >= 0, 45000);
        Thread.Sleep(2000);  // counter-proof window: a rollback should only wind up after the commit count, a redundant delivery shows up here
        List<double> gaps = listener.HeadGaps("rb-head");
        double med = Median(gaps);
        double worst = gaps.Count > 0 ? gaps.Max() : 0;
        var headRecords = listener.Snapshot().Where(r => r.Body == "rb-head").ToList();
        // The index of the 7th delivery in the whole sequence (the head deliveries before idx7 do not count)
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

        Check("S13b-the explicit rollback pushed the same batch back for resending (7 head deliveries = 6 rollbacks + 1 commit)",
            arrived && listener.Count("rb-head") == 7,
            "deliveries=" + listener.Count("rb-head").ToString(CultureInfo.InvariantCulture));
        Check("S13b-a local resend does not go through the broker: the adjacent interval hugs the 200ms suspend (the fastest %RETRY% tier is 10s)",
            gaps.Count >= 5 && med < 1.0 && worst < 5.0,
            "n=" + gaps.Count.ToString(CultureInfo.InvariantCulture)
            + " median=" + med.ToString("F3", CultureInfo.InvariantCulture)
            + "s max=" + worst.ToString("F3", CultureInfo.InvariantCulture) + "s");
        Check("S13b-a local resend does not touch reconsumeTimes and does not change the topic (it did not go through %RETRY%)",
            headRecords.Count > 0 && headRecords.All(r => r.ReconsumeTimes == 0 && r.Topic == topic),
            "times=" + string.Join(",",
                headRecords.Select(r => r.ReconsumeTimes.ToString(CultureInfo.InvariantCulture))));
        Check("S13b-next does not advance before the commit (during the rollback the later message is not let through)",
            idx7 >= 0 && idxNext > idx7,
            "idx7=" + idx7.ToString(CultureInfo.InvariantCulture)
            + " idxNext=" + idxNext.ToString(CultureInfo.InvariantCulture));
        Check("S13b-after winding up, head is exactly 7 times and next exactly 1 time",
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
                Check("S13b-the rollback does not commit the offset early, and after the commit the offset is at 2",
                    ReadCommitted(admin, mq, group) == 2,
                    "committed=" + ReadCommitted(admin, mq, group).ToString(CultureInfo.InvariantCulture));
            }
            else
            {
                Check("S13b-the business topic has a queue whose offset can be queried", false, "queues=0");
            }
        }
        catch (Exception e)
        {
            Check("S13b-the offset query is usable", false, e.Message);
        }

        admin.Shutdown();
    }

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
        Thread.Sleep(2500);  // counter-proof window: if the rollback were really honoured, head would be resent ~10 times in these 2.5s
        Check("S13c-with autoCommit=true a ROLLBACK is treated as an ack: head is delivered only once",
            listener.Count("il-head") == 1,
            "head=" + listener.Count("il-head").ToString(CultureInfo.InvariantCulture));
        Check("S13c-an illegal ROLLBACK does not block the queue: next is consumed at once",
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
                Check("S13c-the broker's offset advances to 2 (the illegal usage counts as a successful ack)",
                    ReadCommitted(admin, mq, group) == 2,
                    "committed=" + ReadCommitted(admin, mq, group).ToString(CultureInfo.InvariantCulture));
            }
            else
            {
                Check("S13c-the business topic has a queue whose offset can be queried", false, "queues=0");
            }
        }
        catch (Exception e)
        {
            Check("S13c-the offset query is usable", false, e.Message);
        }

        admin.Shutdown();
    }

    // ---------------- S14 correctTagsOffset (an empty response corrects the consumed offset)----------------

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

        // ---------- S14a control group: the messages really are there, and normal consumption lands exactly on each queue's maxOffset ----------
        var ctrlListener = new CollectingListenerConcurrently();
        var ctrl = NewConsumer(ctrlGroup);
        ctrl.ConsumeMessageBatchMaxSize = 3;
        ctrl.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
        ctrl.SetMessageListener(ctrlListener);
        ctrl.Subscribe(topic, "TagA");
        ctrl.Start();
        List<MessageQueue> mainQueues = ctrl.FetchSubscribeMessageQueues(topic);
        Check("S14a-the business topic has 4 queues whose offsets can be queried", mainQueues.Count == 4,
            "queues=" + mainQueues.Count.ToString(CultureInfo.InvariantCulture));
        Thread.Sleep(2000);
        for (int i = 0; i < 5; ++i)
        {
            producer.Send(new Message(topic, "TagA", string.Empty,
                Str2Bytes("cto-" + i.ToString(CultureInfo.InvariantCulture))));
        }

        bool ctrlOk = WaitUntil(() => ctrlListener.Snapshot().Count >= 5, 30000);
        Check("S14a-the control group (TagA) received all 5 -- the messages really are in the queues",
            ctrlOk && ctrlListener.Snapshot().Count == 5,
            "arrivals=" + ctrlListener.Snapshot().Count.ToString(CultureInfo.InvariantCulture));

        // committed uses -1 to mean the broker found no record (QUERY_NOT_FOUND); 0 is a legal offset and the two must not be conflated
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
        Check("S14a-the control group's committed offset == each queue's maxOffset (the numeric rule)",
            Matches(mainQueues, ctrlGroup), Text(ctrlRows));
        Check("S14a-the control group really advanced the queues (the maxOffset total is > 0)",
            ctrlRows.Sum(r => r.Max) > 0,
            "maxSum=" + ctrlRows.Sum(r => r.Max).ToString(CultureInfo.InvariantCulture));
        ctrl.Shutdown();

        // ---------- S14b NO_MATCHED_MSG: a subscription that never matches, zero deliveries but the offset still has to move ----------
        var testListener = new CollectingListenerConcurrently();
        var test = NewConsumer(testGroup);
        test.ConsumeMessageBatchMaxSize = 3;
        test.SetMessageListener(testListener);
        test.Subscribe(topic, "TagB");
        test.Start();
        // the 10s first hop plus the 5s offset-persistence cadence, so give the window enough room
        WaitUntil(() => Matches(mainQueues, testGroup), 40000);
        List<(int QueueId, long Max, long Committed)> testRows = Snap(mainQueues, testGroup);
        Check("S14b-zero deliveries (the listener received nothing at all)",
            testListener.Snapshot().Count == 0,
            "arrivals=" + testListener.Snapshot().Count.ToString(CultureInfo.InvariantCulture));
        Check("S14b-every queue's committed offset == that queue's maxOffset (the empty-response correction took effect)",
            Matches(mainQueues, testGroup), Text(testRows));
        Check("S14b-the sum of the corrected offsets == the control group's (the same queue's max offset)",
            testRows.Sum(r => r.Committed) == ctrlRows.Sum(r => r.Max),
            "test=" + testRows.Sum(r => r.Committed).ToString(CultureInfo.InvariantCulture)
            + " ctrl=" + ctrlRows.Sum(r => r.Max).ToString(CultureInfo.InvariantCulture));

        // ---------- S14c NO_NEW_MSG: %RETRY%<group> empty queues must also leave an offset record ----------
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
        Check("S14c-an offset record appears on " + retryTopic + " and equals maxOffset",
            retryQueues.Count > 0 && Matches(retryQueues, testGroup), Text(retryRows));
        Check("S14c-that offset really is 0 (the empty queue's nextBeginOffset)",
            retryRows.Count > 0 && retryRows.All(r => r.Committed == 0),
            "offsets=" + string.Join(",",
                retryRows.Select(r => r.Committed.ToString(CultureInfo.InvariantCulture))));

        // Wait through another quiet window: the correction only raises the offset and must not deliver anything
        Thread.Sleep(6000);
        Check("S14c-over the whole round the listener is still at 0 (the correction does not conjure deliveries)",
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
    /// Live forensics on %DLQ%&lt;group&gt;: first a one-shot probe waits for the broker to
    /// register the dead letter topic in the route, then an **independent consumer group +
    /// FirstOffset** reads out the existing dead letters (a new group with LAST would start
    /// at the tail and skip the one that already landed in the dead letter => a false
    /// failure). routeFound is returned separately: in a negative case "the broker never
    /// created the dead letter topic at all" is itself the correct conclusion and must not be
    /// merged with "the route never arrived" into one empty result.
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
        // SeekToBegin must happen after Start (it throws while not Started), and it moves the
        // pull cursor back to the smallest offset; if the pull loop had already pulled that
        // same queue into the local buffer, Seek only drops the copies "before the offset",
        // and the same message gets delivered twice with the same (QueueId, QueueOffset).
        // Deduplicate by storage position: if there really were two copies in the dead letter
        // they would land on different QueueOffsets and would not be eaten.
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
            // After receiving, drain a few more rounds: asserting "there is only this one in
            // the dead letter" requires seeing the ones behind it too, but the total window
            // has to stay bounded, otherwise every positive case has to sit out the full
            // timeoutMs for nothing.
            long drainTo = NowMs() + 3000;
            while (NowMs() < drainTo) Collect(reader.Poll(500));
            break;
        }
        reader.Shutdown();
        return (true, msgs);
    }
}
