// Real-machine verification of Validators / TopicValidator.
// Usage: rmq validators-live [namesrv]
//
// This tool uses one set of assertions to prove that
// "an illegal name is stopped on the very first local line, while a legal name goes on being
// sent and received by the cluster".
//
// Prerequisite: a NameServer + Broker are up (an ordinary configuration, no ACL/TLS/trace needed).
//
// Scenarios:
//   S1 the send path: blank/oversized/illegal-character topics, the forbidden broker-internal
//      flow topics, the body size tiers, the INNER_MULTI_DISPATCH separator -- all rejected
//      locally in <50ms (with the namesrv sitting right there)
//   S2 the batch path: per-message CheckMessage (an illegal member must also be stopped) plus
//      the homogeneity check
//   S3 the producer's Start(): reserved group / illegal characters / oversized, three gates,
//      plus the equal-length(120) boundary being let through, and a failure not entering the
//      started state
//   S4 positive leg: legal names go on being sent and received by the cluster (a push and a
//      lite consumer each receive 3)
//   S5 control leg: a legal but **non-existent** topic has to go through a real round trip
//      (the broker auto-creates it), an order of magnitude slower than a local reject
//   S6 the group name gates of pull / lite plus querying queues and offsets with a legal pull
//      group
//   S7 the local rejects of CreateTopic (blank / illegal characters / system topic)
//   S8 an addressing failure (10004, not a single name server address) must not be described
//      as "the topic has no route" (10005)
using System.Diagnostics;
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveValidators
{
    /// <summary>The timeout budget (in milliseconds) for local validation: the real round
    /// trip leg of S5 was measured starting at tens of milliseconds.</summary>
    private const double LocalBudgetMs = 50.0;

    private const int QueueNums = 4;

    private static readonly long WallNowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static readonly string Stamp = WallNowMs.ToString(CultureInfo.InvariantCulture);

    private static readonly string Namesrv = "127.0.0.1:9876";

    private static string _namesrv = Namesrv;

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

    private static string Ms(double v) => v.ToString("0.00", CultureInfo.InvariantCulture) + "ms";

    private static string RepeatedChar(char ch, int n) => new string(ch, n);

    private const string IllegalTail =
        " contains illegal characters, allowing only ^[%|a-zA-Z0-9_-]+$";

    private static readonly string FileSeparator =
        Path.DirectorySeparatorChar.ToString();

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string BodyOf(MessageExt m) => Encoding.UTF8.GetString(m.Body);

    /// <summary>
    /// Runs an action and flattens the thrown exception by type into (threw, message, response
    /// code, elapsed). A non-MQClientException (an ArgumentException from a batch, say) records
    /// code 0, meaning "not in the client error code system".
    /// </summary>
    private sealed class Captured
    {
        public bool Threw { get; init; }
        public string What { get; init; } = string.Empty;
        public int Code { get; init; }
        public double ElapsedMs { get; init; }
    }

    private static Captured Capture(Action fn)
    {
        Stopwatch sw = Stopwatch.StartNew();
        try
        {
            fn();
            return new Captured { ElapsedMs = sw.Elapsed.TotalMilliseconds };
        }
        catch (MQClientException e)
        {
            return new Captured
            {
                Threw = true,
                What = e.Message,
                Code = e.ResponseCode,
                ElapsedMs = sw.Elapsed.TotalMilliseconds,
            };
        }
        catch (Exception e)
        {
            return new Captured
            {
                Threw = true,
                What = e.Message,
                ElapsedMs = sw.Elapsed.TotalMilliseconds,
            };
        }
    }

    /// <summary>Asserts "rejected locally": it threw, the message matches, the code is right
    /// and it was not slow enough to look like it went over the network. Returns the measured
    /// elapsed time.</summary>
    private static double ExpectLocalReject(string name, Action fn, string needle, int wantCode)
    {
        Captured c = Capture(fn);
        Check(name,
            c.Threw && c.What.Contains(needle, StringComparison.Ordinal) && c.Code == wantCode
            && c.ElapsedMs < LocalBudgetMs,
         (c.Threw ? c.What : "nothing was thrown") + "  code=" + c.Code.ToString(CultureInfo.InvariantCulture)
            + "  " + Ms(c.ElapsedMs));
        return c.ElapsedMs;
    }

 /// <summary>Asserts "Start() is rejected locally and did not mark itself started after the
 /// failure".</summary>
    private static double ExpectStartReject(string name, Func<bool> startedFlag, Action startFn,
        string needle)
    {
        Captured c = Capture(startFn);
        Check(name, c.Threw && c.What.Contains(needle, StringComparison.Ordinal) && !startedFlag(),
      (c.Threw ? c.What : "nothing was thrown") + "  " + Ms(c.ElapsedMs));
        return c.ElapsedMs;
    }

    private static bool WaitUntil(Func<bool> pred, int timeoutMs)
    {
        long deadline = Stopwatch.GetTimestamp() + (long)(timeoutMs * Stopwatch.Frequency / 1000.0);
        while (Stopwatch.GetTimestamp() < deadline)
        {
            if (pred())
            {
                return true;
            }

            Thread.Sleep(200);
        }

        return pred();
    }

    /// <summary>A route query throws when there is no route (no route / TOPIC_NOT_EXIST); that
    /// is flattened here into "0 queues".</summary>
    private static int RouteQueueCount(Func<List<MessageQueue>> fetch)
    {
        try
        {
            return fetch().Count;
        }
        catch (Exception)
        {
            return 0;
        }
    }

    private sealed class Collector : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<string> _bodies = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk) foreach (MessageExt m in msgs) _bodies.Add(BodyOf(m));
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public List<string> ByPrefix(string prefix)
        {
            lock (_lk) return _bodies.Where(b => b.StartsWith(prefix, StringComparison.Ordinal)).ToList();
        }
    }

    // ---------------- S1 the send path

    private static double S1SendPathRejects(DefaultMQProducer p)
    {
        Console.WriteLine("== S1 the local validation of the send path (with the namesrv present, still no network) ==");
        (string Label, string Topic, string Needle, int Code)[] negs =
        {
 ("S1a an empty topic", "", "The specified topic is blank", ResponseCode.SystemError),
            ("S1b an all-whitespace topic", "   ", "The specified topic is blank", ResponseCode.SystemError),
      ("S1c an oversized topic(128)", RepeatedChar('a', 128), "is longer than topic max length 127",
                ResponseCode.SystemError),
            ("S1d a topic with a dot", "Validators.Live", IllegalTail, ResponseCode.SystemError),
            ("S1e 非 ASCII topic", "Validators中文", IllegalTail, ResponseCode.SystemError),
            ("S1f the forbidden broker-internal flow topic", "SCHEDULE_TOPIC_XXXX", "is forbidden",
                ResponseCode.SystemError),
        };
        double total = 0.0;
        foreach ((string label, string topic, string needle, int code) in negs)
        {
            string t = topic;
            total += ExpectLocalReject(label, () => p.Send(new Message(t, Bytes("body"))), needle, code);
        }

        double local = total / negs.Length;

  ExpectLocalReject("S1g an empty body",
            () => p.Send(new Message("ValidatorsPositive", Array.Empty<byte>())),
            "the message body length is zero", ResponseCode.MessageIllegal);
        ExpectLocalReject("S1h null message",
            () => Validators.CheckMessage(null, 4 << 20),
            "the message is null", ResponseCode.MessageIllegal);
      ExpectLocalReject("S1i over maxMessageSize",
            () => Validators.CheckMessage(new Message("ValidatorsPositive", Bytes("12345")), 4),
            "the message body size over max value, MAX: 4", ResponseCode.MessageIllegal);
        Check("S1j exactly maxMessageSize is let through",
            !Capture(() => Validators.CheckMessage(
                new Message("ValidatorsPositive", Bytes("1234")), 4)).Threw);

        Message lmq = new("ValidatorsPositive", Bytes("hello"));
        lmq.SetUserProperty(MessageConst.PropertyInnerMultiDispatch, "a" + FileSeparator + "b");
     ExpectLocalReject("S1k INNER_MULTI_DISPATCH with a path separator", () => p.Send(lmq),
            "INNER_MULTI_DISPATCH", ResponseCode.MessageIllegal);
        Message legalLmq = new("ValidatorsPositive", Bytes("hello"));
        legalLmq.SetUserProperty(MessageConst.PropertyInnerMultiDispatch, "%LMQ%queue:testCID");
    Check("S1l an ordinary LMQ value is not killed by mistake",
            !Capture(() => Validators.CheckMessage(legalLmq, 4 << 20)).Threw);

        // The order: topic -> forbidden list -> body. An empty body plus an illegal topic
        // must report the topic
        Captured order = Capture(() => p.Send(new Message("bad topic", Array.Empty<byte>())));
  Check("S1m the validation order: topic before body",
            order.Threw && order.What.Contains("contains illegal characters", StringComparison.Ordinal),
     order.Threw ? order.What : "nothing was thrown");
        return local;
    }

    // ---------------- S2 batch

    private static void S2BatchRejects(DefaultMQProducer p, string topic)
    {
        Console.WriteLine("== S2 the per-message validation of the batch path ==");
  ExpectLocalReject("S2a one illegal topic inside the batch",
            () => p.SendBatch(new List<Message>
            {
                new(topic, Bytes("ok-1")),
                new("bad.topic", Bytes("ok-2")),
            }),
            "The specified topic[bad.topic]" + IllegalTail, ResponseCode.SystemError);

        ExpectLocalReject("S2b one empty body inside the batch",
            () => p.SendBatch(new List<Message>
            {
                new(topic, Bytes("ok-1")),
                new(topic, Array.Empty<byte>()),
            }),
            "the message body length is zero", ResponseCode.MessageIllegal);

        Captured mixed = Capture(() => p.SendBatch(new List<Message>
        {
            new(topic, Bytes("ok-1")),
            new(topic + "Other", Bytes("ok-2")),
        }));
        Check("S2c a batch whose topics are not homogeneous is rejected (the non-MQClientException rule)",
            mixed.Threw && mixed.What.Contains("should be the same", StringComparison.Ordinal)
            && mixed.Code == 0 && mixed.ElapsedMs < LocalBudgetMs,
     mixed.Threw ? mixed.What : "nothing was thrown");
    }

    // ---------------- S3 the producer's group name gates

    private static void S3ProducerGroupGates()
    {
        Console.WriteLine("== S3 the group name validation gates of the producer's Start() ==");
        (string Label, string Group, string Needle)[] negs =
        {
        ("S3a the reserved group DEFAULT_PRODUCER", MixAll.DefaultProducerGroup,
                "producerGroup can not equal DEFAULT_PRODUCER, please specify another one."),
       ("S3b a group with a space", "bad group", "the specified group[bad group]" + IllegalTail),
   ("S3c an oversized group(121)", RepeatedChar('g', 121), "is longer than group max length: 120"),
        };
        foreach ((string label, string group, string needle) in negs)
        {
       // the namesrv pointing at a **real** cluster is rejected all the same -- what it blocks
     // is the very first local line, unrelated to network reachability
            var prod = new DefaultMQProducer(group) { NamesrvAddr = _namesrv };
            ExpectStartReject(label, () => prod.IsStarted, () => prod.Start(), needle);
        }

        var edge = new DefaultMQProducer(RepeatedChar('g', 120)) { NamesrvAddr = _namesrv };
        Captured c = Capture(edge.Start);
        Check("S3d a 120-character group name is let through (the length test is > and not >=)",
            !c.Threw || !c.What.Contains("group max length", StringComparison.Ordinal),
        c.Threw ? c.What : "Start did not fail because of the group name length");
        if (edge.IsStarted)
        {
            edge.Shutdown();
        }
    }

    // ---------------- S4 the positive leg

    private static void S4PositiveLeg(DefaultMQProducer p, string topic, string group, string prefix)
    {
        Console.WriteLine("== S4 the positive leg: legal names go on being sent and received by the cluster ==");
        var collector = new Collector();
        var cons = new DefaultMQPushConsumer(group);
        cons.SetNamesrvAddr(_namesrv);
        cons.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromLastOffset;
        cons.Subscribe(topic, "*");
        cons.SetMessageListener(collector);
        Captured started = Capture(cons.Start);
        Check("S4a the push consumer with a legal group name can start", !started.Threw,
            started.Threw ? started.What : "ok");
        if (started.Threw)
        {
            return;
        }

        var lite = new DefaultLitePullConsumer(group + "_lite");
        lite.SetNamesrvAddr(_namesrv);
        lite.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromLastOffset);
        lite.Subscribe(topic, "*");
        Captured liteStarted = Capture(lite.Start);
        Check("S4b the lite consumer with a legal group name can start", !liteStarted.Threw,
            liteStarted.Threw ? liteStarted.What : "ok");

        // The consumer starts before the messages are sent: a new group plus
        // ConsumeFromLastOffset takes the tail offset **at the moment of startup**
        bool assigned = WaitUntil(() => lite.Assignment().Count > 0, 20000);
 Check("S4c lite got a queue assignment", assigned,
        lite.Assignment().Count.ToString(CultureInfo.InvariantCulture) + " queues");

        for (int i = 0; i < 3; i++)
        {
            int n = i;
            Captured s = Capture(() => p.Send(new Message(topic, Bytes(prefix + "-body-" + n)), 5000));
     Check("S4d sending on a legal topic succeeded #" + n.ToString(CultureInfo.InvariantCulture), !s.Threw,
                s.Threw ? s.What : "SEND_OK");
        }

        bool gotAll = WaitUntil(() => collector.ByPrefix(prefix).Count >= 3, 30000);
   Check("S4e the push consumer received all 3", gotAll,
          "received " + collector.ByPrefix(prefix).Count.ToString(CultureInfo.InvariantCulture) + " messages");

        int polled = 0;
        long deadline = Stopwatch.GetTimestamp() + 20L * Stopwatch.Frequency;
        while (polled < 3 && Stopwatch.GetTimestamp() < deadline)
        {
            polled += lite.Poll(1000).Count;
        }

    Check("S4f the lite consumer polled all 3", polled >= 3,
        "polled " + polled.ToString(CultureInfo.InvariantCulture) + " messages");
        lite.Shutdown();
        cons.Shutdown();
    }

    // ---------------- S5 the control leg

    private static void S5ControlLeg(DefaultMQProducer p, double localMs)
    {
        Console.WriteLine("== S5 the control leg: what the local validation saves ==");
        // A legal but **non-existent** topic: let through locally -> a real round trip (the
        // broker auto-creates it off TBW102)
        string missing = "ValidatorsMissing" + Stamp;
        Captured c = Capture(() => p.Send(new Message(missing, Bytes("x")), 5000));
    Check("S5a a legal but non-existent topic is not killed by mistake locally (the broker auto-creates it)",
            !c.Threw || !c.What.Contains(IllegalTail, StringComparison.Ordinal),
            (c.Threw ? c.What : "SEND_OK") + "  " + Ms(c.ElapsedMs));
        double baseline = localMs < 0.05 ? 0.05 : localMs;
  Check("S5b the cluster leg is more than an order of magnitude slower than the local negative leg", c.ElapsedMs > 10.0 * baseline,
            Ms(c.ElapsedMs) + " vs local " + Ms(baseline));
    }

    // ---------------- S6 the consumer side

    private static void S6ConsumerGates(string topic)
    {
        Console.WriteLine("== S6 the group name gates of pull/lite + querying offsets with a legal pull group ==");
        var pullReserved = new DefaultMQPullConsumer(MixAll.DefaultConsumerGroup);
        pullReserved.SetNamesrvAddr(_namesrv);
        ExpectStartReject("S6a a pull consumer blocks DEFAULT_CONSUMER", () => pullReserved.IsStarted,
            pullReserved.Start,
            "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");

        var liteReserved = new DefaultLitePullConsumer(MixAll.DefaultConsumerGroup);
        liteReserved.SetNamesrvAddr(_namesrv);
        liteReserved.Subscribe(topic, "*");
        ExpectStartReject("S6b a lite consumer blocks DEFAULT_CONSUMER", () => liteReserved.IsStarted,
            liteReserved.Start,
            "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");

        var liteIllegal = new DefaultLitePullConsumer("bad group");
        liteIllegal.SetNamesrvAddr(_namesrv);
        liteIllegal.Subscribe(topic, "*");
        ExpectStartReject("S6c a lite consumer blocks an illegal-character group", () => liteIllegal.IsStarted,
            liteIllegal.Start, "the specified group[bad group]" + IllegalTail);

        var pull = new DefaultMQPullConsumer("GID_validators_pull_" + Stamp);
        pull.SetNamesrvAddr(_namesrv);
        Captured started = Capture(pull.Start);
        bool ok = !started.Threw;
        int queues = 0;
        long maxOffset = -1;
        if (ok)
        {
            queues = RouteQueueCount(() => pull.FetchSubscribeMessageQueues(topic));
            if (queues > 0)
            {
                MessageQueue first = pull.FetchSubscribeMessageQueues(topic)[0];
                maxOffset = pull.MaxOffset(first);
            }
        }

        Check("S6d the pull consumer with a legal group name starts and queries its offsets",
            ok && queues >= QueueNums && maxOffset >= 0,
            started.Threw ? started.What : "queues=" + queues + " maxOffset=" + maxOffset);
        if (ok)
        {
            pull.Shutdown();
        }
    }

    // ---------------- S7 CreateTopic

    private static void S7CreateTopicRejects(DefaultMQProducer p)
    {
        Console.WriteLine("== S7 the local rejects of CreateTopic ==");
    ExpectLocalReject("S7a CreateTopic blocks illegal characters",
            () => p.CreateTopic(MixAll.DefaultTopic, "bad topic", QueueNums),
            "The specified topic[bad topic]" + IllegalTail, ResponseCode.SystemError);
  ExpectLocalReject("S7b CreateTopic blocks a system topic",
            () => p.CreateTopic(MixAll.DefaultTopic, "RMQ_SYS_TRACE_TOPIC", QueueNums),
            "is conflict with system topic", ResponseCode.SystemError);
   ExpectLocalReject("S7c CreateTopic blocks an empty topic",
            () => p.CreateTopic(MixAll.DefaultTopic, "", QueueNums),
            "The specified topic is blank", ResponseCode.SystemError);
    }

    public static int Run(string[] args)
    {
        if (args.Length > 0 && args[0].Trim().Length > 0)
        {
            _namesrv = args[0].Trim();
        }

        string topic = "ValidatorsNet_" + Stamp;
        string group = "GID_validators_net_" + Stamp;
        string prefix = "validators-net-" + Stamp;

        Console.WriteLine("=== RocketMQ validators live verify (C#) ===");
        Console.WriteLine("namesrv=" + _namesrv + " stamp=" + Stamp);

        var producer = new DefaultMQProducer("GID_validators_net_producer_" + Stamp)
        {
            NamesrvAddr = _namesrv,
        };
        Captured started = Capture(producer.Start);
        if (started.Threw)
        {
            Check("S0 the producer started", false, started.What);
            Report();
            return 1;
        }

        Check("S0 the producer started", true, "group=" + producer.ProducerGroup);

        double localMs = S1SendPathRejects(producer);
        S2BatchRejects(producer, topic);
        S3ProducerGroupGates();

        Captured created = Capture(() => producer.CreateTopic(MixAll.DefaultTopic, topic, QueueNums));
        if (created.Threw)
        {
       Check("S4 precondition: the topic was created", false, created.What);
            Report();
            producer.Shutdown();
            return 1;
        }

        bool routed = WaitUntil(
            () => RouteQueueCount(() => producer.FetchPublishMessageQueues(topic)) >= QueueNums,
            20000);
   Check("S4 precondition: the route is discoverable", routed,
            "queues=" + RouteQueueCount(() => producer.FetchPublishMessageQueues(topic)));
        if (routed)
        {
            S4PositiveLeg(producer, topic, group, prefix);
            S6ConsumerGates(topic);
        }

        S5ControlLeg(producer, localMs);
        S7CreateTopicRejects(producer);
        if (routed)
        {
   // S8 needs an **existing** topic: the only thing broken must be the addressing table
            S8NameServerAddressing(topic);
        }

        producer.Shutdown();
        Report();
        return _fail == 0 ? 0 : 1;
    }

    /// <summary>
    /// S8: an addressing failure (10004) must not masquerade as "the topic has no route" (10005).
    private static void S8NameServerAddressing(string topic)
    {
        Console.WriteLine("== S8 the boundary between 10004 (no name server configured) and 10005 (the topic has no route) ==");
        var p = new DefaultMQProducer("PID_ValidatorsLive_Addr")
        {
            NamesrvAddr = Namesrv,
            InstanceName = "ValidatorsAddr",
        };
        p.Start();
        try
        {
            // NameServerAddrs returns the instance's internal List itself, so Clear empties
      // it: a live producer plus an **existing** topic, with the addressing table being the
    // only thing broken.
            var addrs = (List<string>)p.Client().NameServerAddrs;
            var saved = new List<string>(addrs);
            addrs.Clear();

            var sw = Stopwatch.StartNew();
            MQClientException? e = null;
            try
            {
                p.Send(new Message(topic, Bytes("no-name-server")));
            }
            catch (MQClientException ex)
            {
                e = ex;
            }

            double elapsedMs = sw.Elapsed.TotalMilliseconds;
  Check("S8a with not a single address it reports 10004 NO_NAME_SERVER_EXCEPTION (not 10005)",
                e is not null && e.ResponseCode == ClientErrorCode.NoNameServerException,
                "code=" + e?.ResponseCode + " msg=" + e?.Message);
            Check("S8b the message is verbatim as agreed by the protocol: 'No name server address, please set it.' (it points at addressing)",
                e?.Message == "No name server address, please set it.", "msg=" + e?.Message);
  Check("S8c the failure happens in the local decision without burning the retry budget (<"
    + LocalBudgetMs.ToString(CultureInfo.InvariantCulture) + "ms)",
                elapsedMs < LocalBudgetMs, "elapsed=" + elapsedMs.ToString("F2", CultureInfo.InvariantCulture) + "ms");

            addrs.AddRange(saved);
            SendResult r = p.Send(new Message(topic, Bytes("name-server-back")));
        Check("S8d the control: once the addresses are back the very same topic gets SEND_OK at once (showing S8a judged addressing, not the topic)",
                r.SendStatus == SendStatus.SendOk, "status=" + r.SendStatus);
        }
        finally
        {
            p.Shutdown();
        }
    }

    private static void Report()
    {
        Console.WriteLine("############ PASS=" + _pass + " FAIL=" + _fail + " ############");
    }
}
