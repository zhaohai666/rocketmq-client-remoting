// The offline unit tests (tests/FlowControlTests.cs) pin the **criteria themselves**; what
// is pinned here are the two things offline can never pin:
//   A The gate **really does hit** on a real broker. A unit off by one place or a threshold
//     read from the wrong field still "hits" offline against a seeded buffer, while on a
//     real machine it never hits (or always hits). A misalignment like wiring the topic-level
//     gate to the queue-level switch stays entirely green offline.
//   B After a hit, **not one message may be lost**. Flow control only "pauses pulling", it
//     does not "drop/skip": while paused the offset must not pass a message that has not been
//     consumed, and after resuming the same queue must keep consuming to the end. Writing it
//     as "drop the batch / leave the loop on a hit" is invisible in a ten-second window and
//     only shows up once every message has been counted.
//
//
// Note: large messages must be **incompressible**: the producer tries to compress bodies
//   over the compression threshold first, and an all-identical-byte payload compresses down
//   to a few hundred bytes, which changes the StoreSize the broker persists by the same
//   amount -- "the size gate never hits" then becomes a fixture problem (this is the easiest
//   one to hit on a real machine's first round).
// Note: the topics of S1/S4 must have **exactly 1 queue**: spreading 8 x 400KB over 4 queues
//   gives only 800KB each, which never reaches the 1MiB **queue-level** gate.
//
// Usage: rmq flow-control [namesrv] (needs a local 5.5.1 cluster)
using System.Security.Cryptography;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveFlowControl
{
    private static string _namesrv = "127.0.0.1:9876";
    private static string _stamp = string.Empty;
    private static DefaultMQAdminExt _admin = null!;

    private static int _pass;
    private static int _fail;

    private const int Huge = 65535;

   /// <summary>How the byte gate is "switched off" is the same: pullThresholdSizeForQueue's legal range is [1, 1024] MiB.</summary>
    private const int HugeSizeMiB = 1024;

    private const int KiB = 1024;

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

    private static void Sleep(int ms) => Thread.Sleep(ms);

    private static bool WaitUntil(Func<bool> pred, int millis)
    {
        DateTime deadline = DateTime.UtcNow.AddMilliseconds(millis);
        for (;;)
        {
            if (pred()) return true;
            if (DateTime.UtcNow >= deadline) return false;
            Sleep(100);
        }
    }

    private static string Topic(string kind) => "FcCs_" + _stamp + "_" + kind;

    private static string Group(string kind) => "fc_cs_" + _stamp + "_g" + kind;

  /// <summary>Explicitly creates the topic (not relying on autoCreateTopicEnable, so the queue
  /// count is certain -- S1 relies on "it is exactly 1 queue").</summary>
    private static void PrepareTopic(DefaultMQProducer producer, string topic, int queues)
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
        Sleep(3000);
    }

   /// <summary>A 400KB **incompressible** message body: a prefix plus a random tail (the prefix only makes it recognisable in logs, the criteria always use counts).</summary>
    private static byte[] BigBody(int tag)
    {
        byte[] body = new byte[400 * KiB];
        RandomNumberGenerator.Fill(body);
        byte[] prefix = Encoding.UTF8.GetBytes("FCBIG-" + tag.ToString("D3") + "-");
        Array.Copy(prefix, body, prefix.Length);
        return body;
    }

    /// <summary>Used for consuming: records the body and optionally sleeps slowMs per batch
    /// (to create a "pulled but not consumed" backlog).</summary>
    private sealed class Sink : IMessageListenerConcurrently
    {
        private readonly int _slowMs;
        private readonly object _lk = new();
        private readonly HashSet<string> _bodies = new(StringComparer.Ordinal);
        private int _got;

        public Sink(int slowMs) => _slowMs = slowMs;

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
        {
            if (_slowMs > 0) Sleep(_slowMs);
            lock (_lk)
            {
                foreach (MessageExt m in msgs)
                {
           // use length + prefix as the key: putting a whole 400KB body into a HashSet only wastes memory
                    _bodies.Add(m.Body.Length + ":" + Encoding.UTF8.GetString(m.Body, 0,
                        Math.Min(m.Body.Length, 16)));
                    ++_got;
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public int Got()
        {
            lock (_lk) return _got;
        }

        public int Distinct()
        {
            lock (_lk) return _bodies.Count;
        }
    }

    private sealed class Case
    {
        public required DefaultMQPushConsumer Consumer;
        public required Sink Sink;
    }

    private static Case StartConsumer(string kind, string topic, int slowMs)
    {
        var sink = new Sink(slowMs);
        var consumer = new DefaultMQPushConsumer(Group(kind));
        consumer.SetNamesrvAddr(_namesrv);
        consumer.InstanceName = "live-fc-" + kind + "-" + _stamp;
        consumer.Subscribe(topic, "*");
        consumer.SetMessageListener(sink);
        return new Case { Consumer = consumer, Sink = sink };
    }

    private static void SendSmall(DefaultMQProducer producer, string topic, int n)
    {
        for (int i = 0; i < n; ++i)
        {
            producer.Send(new Message(topic, Encoding.UTF8.GetBytes("ok-" + i)), 5000);
        }
    }

    private static void SendBig(DefaultMQProducer producer, string topic, int n)
    {
        for (int i = 0; i < n; ++i)
        {
            producer.Send(new Message(topic, BigBody(i)), 20000);
        }
    }

      // ------------------------------------------------ S0 the default gates + fast consumption
    private static void S0Defaults(DefaultMQProducer producer)
    {
        string topic = Topic("Defaults");
        PrepareTopic(producer, topic, 4);
        Case c = StartConsumer("0", topic, 0);
        c.Consumer.Start();
        Sleep(3000);
        SendSmall(producer, topic, 12);
        bool all = WaitUntil(() => c.Sink.Got() >= 12, 30000);
        long fc = c.Consumer.FlowControlTriggered;
        c.Consumer.Shutdown();
        Check("S0-the default gates do not hit", fc == 0, "triggered=" + fc);
      Check("S0-under the default gates everything arrives", all && c.Sink.Got() == 12, "got=" + c.Sink.Got());
        Check("S0-nothing duplicated and nothing lost", c.Sink.Distinct() == 12, "distinct=" + c.Sink.Distinct());
    }

  // ------------------------------ S1 the queue-level byte gate (the topic must be single-queue)
    private static void S1SizeGate(DefaultMQProducer producer)
    {
        string topic = Topic("Size");
        PrepareTopic(producer, topic, 1);
        Case c = StartConsumer("1", topic, 300);
        c.Consumer.PullThresholdForQueue = Huge;
        c.Consumer.PullThresholdSizeForQueue = 1;      // 1 MiB
        c.Consumer.ConsumeConcurrentlyMaxSpan = Huge;
        c.Consumer.Start();
        Sleep(3000);
        SendBig(producer, topic, 8);
        bool hit = WaitUntil(() => c.Consumer.FlowControlTriggered > 0, 30000);
        bool all = WaitUntil(() => c.Sink.Got() >= 8, 40000);
        long fc = c.Consumer.FlowControlTriggered;
        c.Consumer.Shutdown();
     Check("S1-the queue-level byte gate really hits on a real machine", hit, "triggered=" + fc);
        Check("S1-no large message is lost", all && c.Sink.Got() == 8, "got=" + c.Sink.Got());
        Check("S1-the 8 400KB messages are all distinct", c.Sink.Distinct() == 8,
            "distinct=" + c.Sink.Distinct());
    }

      // ------------------------------------------------------ S2 the offset span gate
    private static void S2SpanGate(DefaultMQProducer producer)
    {
        string topic = Topic("Span");
        PrepareTopic(producer, topic, 4);
        Case c = StartConsumer("2", topic, 300);
        c.Consumer.PullThresholdForQueue = Huge;
        c.Consumer.PullThresholdSizeForQueue = HugeSizeMiB;
        c.Consumer.ConsumeConcurrentlyMaxSpan = 2;
        c.Consumer.Start();
        Sleep(3000);
        SendSmall(producer, topic, 14);
        bool hit = WaitUntil(() => c.Consumer.FlowControlTriggered > 0, 30000);
        bool all = WaitUntil(() => c.Sink.Got() >= 14, 40000);
        long fc = c.Consumer.FlowControlTriggered;
        c.Consumer.Shutdown();
        Check("S2-the span gate really hits on a real machine", hit, "triggered=" + fc);
        Check("S2-everything is still consumed after the span limit", all && c.Sink.Got() == 14, "got=" + c.Sink.Got());
    }

      // --------------------------- S3 the topic-level count budget: DIVIDED, not summed
    private static void S3TopicGate(DefaultMQProducer producer)
    {
        string topic = Topic("TopicCount");
        PrepareTopic(producer, topic, 4);
        Case c = StartConsumer("3", topic, 300);
        // The two per-queue values the user set: while a topic budget is enabled the count
        // knob is overwritten by the division, while the byte budget being off (-1) must
        // leave the byte knob exactly as it was set.
        c.Consumer.PullThresholdForQueue = Huge;
        c.Consumer.PullThresholdSizeForQueue = HugeSizeMiB;
        c.Consumer.ConsumeConcurrentlyMaxSpan = Huge;
        c.Consumer.PullThresholdForTopic = 4;
        c.Consumer.PullThresholdSizeForTopic = -1;
        c.Consumer.Start();
        int sizeKnob = c.Consumer.PullThresholdSizeForQueue;
        int countKnob = c.Consumer.PullThresholdForQueue;
        Sleep(3000);
        SendSmall(producer, topic, 16);
        bool hit = WaitUntil(() => c.Consumer.FlowControlTriggered > 0, 30000);
        bool all = WaitUntil(() => c.Sink.Got() >= 16, 40000);
        long fc = c.Consumer.FlowControlTriggered;
        c.Consumer.Shutdown();
        // What the topic budget changes is the **divided per-queue knob**, never a pull-time
        // topic total: 4 messages over the queues this instance owns (the 4 topic queues plus
        // the auto-subscribed %RETRY% queues) is at most 4 and at least 1, and it is strictly
        // below the user's Huge value because rebalance overwrote it.
        Check("S3-the topic budget is divided into the per-queue count knob",
            countKnob <= 4 && countKnob >= 1,
            "wants [1, 4], PullThresholdForQueue=" + countKnob + " (user value was " + Huge + ")");
        Check("S3-an off topic byte budget leaves the byte knob alone",
            sizeKnob == HugeSizeMiB, "PullThresholdSizeForQueue=" + sizeKnob);
        // The gate trips on **one queue's own buffer** crossing that divided share.
        Check("S3-one queue's own buffer really hits the divided gate", hit, "triggered=" + fc);
        Check("S3-everything is still consumed after a hit (a pause is not a drop)",
            all && c.Sink.Got() == 16, "got=" + c.Sink.Got());
        Check("S3-nothing duplicated and nothing lost", c.Sink.Distinct() == 16,
            "distinct=" + c.Sink.Distinct());
    }

    // ------------------------------------- S4 recovery after a hit (reusing S1's group and topic)
    private static void S4Recovery(DefaultMQProducer producer)
    {
   // The offset was already committed to the broker's tail by S1, so only a new batch arrives here.
        string topic = Topic("Size");
        Case c = StartConsumer("1", topic, 300);
        c.Consumer.PullThresholdForQueue = Huge;
        c.Consumer.PullThresholdSizeForQueue = 1;
        c.Consumer.ConsumeConcurrentlyMaxSpan = Huge;
        c.Consumer.Start();
        Sleep(3000);
        SendBig(producer, topic, 6);
        bool all = WaitUntil(() => c.Sink.Got() >= 6, 40000);
        bool hit = WaitUntil(() => c.Consumer.FlowControlTriggered > 0, 30000);
        long fc = c.Consumer.FlowControlTriggered;
        c.Consumer.Shutdown();
      Check("S4-a queue that triggered flow control keeps consuming after recovery", all && c.Sink.Got() == 6,
            "got=" + c.Sink.Got());
    Check("S4-the recovery batch still hits flow control (the gate does not become ineffective after one hit)", hit, "triggered=" + fc);
        Check("S4-the recovery batch has no duplicates", c.Sink.Distinct() == 6, "distinct=" + c.Sink.Distinct());
    }

    private static void S5CheckConfig(DefaultMQProducer producer)
    {
        string topic = Topic("Config");
        PrepareTopic(producer, topic, 4);
        string goodGroup = Group("5");
        string badGroup = Group("6");

        Case c = StartConsumer("5", topic, 0);
        c.Consumer.SetConsumeThreadMin(1);
        c.Consumer.SetConsumeThreadMax(2);
        c.Consumer.ConsumeConcurrentlyMaxSpan = Huge;
        c.Consumer.PullThresholdForQueue = Huge;
        c.Consumer.PullThresholdForTopic = -1;
        c.Consumer.PullThresholdSizeForQueue = HugeSizeMiB;
        c.Consumer.PullThresholdSizeForTopic = -1;
        c.Consumer.PullIntervalMillis = 0;
        c.Consumer.ConsumeMessageBatchMaxSize = 1;
        c.Consumer.PullBatchSize = 1024;
        c.Consumer.PopInvisibleTime = DefaultMQPushConsumer.MaxPopInvisibleTime;
        c.Consumer.PopBatchNums = 32;
        bool started = true;
        try
        {
            c.Consumer.Start();
        }
        catch (Exception e)
        {
            started = false;
     Check("S5-the boundary-value configuration starts", false, e.Message);
        }

        if (started)
        {
  Check("S5-the boundary-value configuration starts", true, "IsStarted=" + c.Consumer.IsStarted);
            Sleep(3000);
            for (int i = 0; i < 10; ++i)
            {
                producer.Send(new Message(topic, Encoding.UTF8.GetBytes("c-" + i)), 5000);
            }

            bool all = WaitUntil(() => c.Sink.Got() >= 10, 30000);
     Check("S5-all 10 arrive under the boundary-value configuration",
                all && c.Sink.Got() == 10 && c.Sink.Distinct() == 10,
                "got=" + c.Sink.Got() + " distinct=" + c.Sink.Distinct());
        }

        (Action<DefaultMQPushConsumer> Break, string Want)[] bad =
        {
            (x => x.PullThresholdSizeForQueue = 0,
             "pullThresholdSizeForQueue Out of range [1, 1024]"),
            (x => x.PullBatchSize = 1025, "pullBatchSize Out of range [1, 1024]"),
            (x => x.PopInvisibleTime = DefaultMQPushConsumer.MinPopInvisibleTime - 1,
             "popInvisibleTime Out of range [5000, 300000]"),
            (x => x.PopBatchNums = 33, "popBatchNums Out of range [1, 32]"),
            (x =>
            {
                x.SetConsumeThreadMin(8);
                x.SetConsumeThreadMax(4);
            }, "consumeThreadMin (8) is larger than consumeThreadMax (4)"),
        };
        foreach ((Action<DefaultMQPushConsumer> brk, string want) in bad)
        {
            var badConsumer = new DefaultMQPushConsumer(badGroup)
            {
                InstanceName = "live-fc-bad-" + _stamp,
            };
            badConsumer.SetNamesrvAddr(_namesrv);
            badConsumer.Subscribe(topic, "*");
            badConsumer.SetMessageListener(new Sink(0));
            brk(badConsumer);
            bool rejected = false;
            string actual;
            try
            {
                badConsumer.Start();
     actual = "Start() unexpectedly succeeded";
                badConsumer.Shutdown();
            }
            catch (MQClientException e)
            {
                rejected = true;
                actual = e.Message;
            }
            catch (Exception e)
            {
       actual = "a different exception: " + e.Message;
            }

          Check("S5-the out-of-range configuration is rejected: " + want, rejected && actual == want, "actually=" + actual);
          Check("S5-the out-of-range configuration left no half-started instance: " + want, !badConsumer.IsStarted);
        }

        if (!started)
        {
            return;
        }

        // The broker-side counter-proof: the rejected group is not findable while the
        // boundary-value group is. It has to use the **bare** GetConsumerListByGroup --
    // GetConsumerIdListByGroup swallows the exception internally and returns null, which
        // makes "rejected" and "not registered" look identical to the caller (the same note
        // in S6 of LiveAcl.cs).
        var probe = new MQClientInstance("FC_CS_PROBE_" + _stamp, new List<string> { _namesrv });
        probe.Start();
        try
        {
            string addr = probe.BrokerAddrForTopic(topic);
        Check("S5-got a broker address for querying the consumer group", addr.Length > 0, "addr=" + addr);
            if (addr.Length == 0)
            {
                return;
            }

      // A group that never registered: the broker does not reply with an empty list but
        // throws code=1 "no consumer for this group" straight away. Both shapes count as
    // "no such group", but it must never carry a clientId.
            bool absent;
            string detail;
            try
            {
                var ids = probe.GetConsumerListByGroup(badGroup, addr, 5000);
                absent = ids.ConsumerIdList.Count == 0;
                detail = "ids=" + ids.ConsumerIdList.Count;
            }
            catch (MQBrokerException e)
            {
                absent = true;
     detail = "the broker refused straight away: code=" + e.ResponseCode + " " + e.ResponseMessage;
            }
            catch (Exception e)
            {
                absent = false;
         detail = "the probe request failed: " + e.Message;
            }

            Check("S5-the broker side does not know the rejected consumer group", absent, detail);

            int goodN = 0;
            string goodDetail = string.Empty;
            try
            {
                goodN = probe.GetConsumerListByGroup(goodGroup, addr, 5000).ConsumerIdList.Count;
            }
            catch (Exception e)
            {
     goodDetail = "the probe failed: " + e.Message;
            }

      Check("S5-the broker side accepted the boundary-value consumer", goodN == 1,
                "n=" + goodN + (goodDetail.Length > 0 ? " " + goodDetail : ""));
        }
        finally
        {
            probe.Shutdown();
            c.Consumer.Shutdown();
        }
    }

    private static void Cleanup()
    {
        foreach (string kind in new[] { "Defaults", "Size", "Span", "TopicCount", "Config" })
        {
            try
            {
                _admin.DeleteTopic(Topic(kind));
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] DeleteTopic(" + Topic(kind) + ") failed: " + e.Message);
            }
        }
    }

    public static int Run(string[] args)
    {
        _namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        _stamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();
  Console.WriteLine("== flow control real-machine verification: " + _namesrv + " stamp=" + _stamp + " ==");

        var producer = new DefaultMQProducer("PID_fc_cs_" + _stamp)
        {
            NamesrvAddr = _namesrv,
            InstanceName = "fc-cs-producer-" + _stamp,
            SendMsgTimeout = 20000,
        };
        producer.Start();
        _admin = new DefaultMQAdminExt("FCCSADMIN");
        _admin.SetNamesrvAddr(_namesrv);
        _admin.SetTimeoutMillis(10000);
        _admin.Start();

        try
        {
     // cluster liveness: without a broker registration do not go any further, otherwise every
       // criterion is just waiting out a timeout
            bool alive = WaitUntil(() =>
            {
                try
                {
                    return producer.FetchPublishMessageQueues(MixAll.DefaultTopic).Count > 0;
                }
                catch (Exception)
                {
                    return false;
                }
            }, 30000);
            if (!alive)
            {
     Check("the cluster liveness probe", false, "the nameServer has no route for " + MixAll.DefaultTopic);
            }
            else
            {
   Check("the cluster liveness probe", true, "the broker's queue count=" +
                    producer.FetchPublishMessageQueues(MixAll.DefaultTopic).Count);
                S0Defaults(producer);
                S1SizeGate(producer);
                S2SpanGate(producer);
                S3TopicGate(producer);
                S4Recovery(producer);
                S5CheckConfig(producer);
            }
        }
        catch (Exception e)
        {
            Check("the integration threw", false, e.Message);
        }

        Cleanup();
        producer.Shutdown();
        _admin.Shutdown();
        Console.WriteLine();
        Console.WriteLine("PASS=" + Volatile.Read(ref _pass) + " FAIL=" + Volatile.Read(ref _fail));
        return Volatile.Read(ref _fail) == 0 ? 0 : 1;
    }
}
