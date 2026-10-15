// Real-machine integration of the C# client's SQL92 filtering + CHECK_CLIENT_CONFIG(46)
// (scenarios S1-S4 verified section by section; the offline assertions on the wire shape are
// in CheckClientConfigTests).
//
// Prerequisite: a local 5.5.1 cluster is up and the broker is configured with
// enablePropertyFilter=true.
// Usage: rmq sql92 [namesrv]
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveSql92
{
    private static readonly string Stamp =
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    private static string _nsAddr = "127.0.0.1:9876";

    private static int _pass;
    private static int _fail;

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok)
        {
            _pass++;
        }
        else
        {
            _fail++;
        }

        Console.WriteLine("  " + (ok ? "[PASS]" : "[FAIL]") + " " + name
                                                    + (detail.Length == 0 ? "" : "  " + detail));
    }

    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 300)
    {
        long deadline = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() + timeoutMs;
        while (DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(intervalMs);
        }

        return pred();
    }

    private static string BodyText(MessageExt m) => Encoding.UTF8.GetString(m.Body);

    private static string Join(IEnumerable<string> items) =>
        "[" + string.Join(",", items) + "]";

    // ---------------- the evidence hook ----------------

    /// <summary>
  /// Hung on the transport, capturing the 46 requests really sent during startup (including
  /// the body).
  /// DoBeforeRequest runs on the sending thread and one hook object may be shared by several
  /// threads, so it must bring its own lock.
    /// </summary>
    private sealed class CheckConfigProbe : IRpcHook
    {
        private readonly object _lk = new();
        private readonly List<int> _codes = new();
        private readonly List<CheckClientRequestBody> _bodies = new();

        public void DoBeforeRequest(string remoteAddr, RemotingCommand request)
        {
            lock (_lk)
            {
                _codes.Add(request.Code);
                if (request.Code != RequestCode.CheckClientConfig)
                {
                    return;
                }

                if (request.Body.Length > 0
                    && CheckClientRequestBody.Decode(request.Body, out CheckClientRequestBody b))
                {
                    _bodies.Add(b);
                }
            }
        }

        public void DoAfterResponse(string remoteAddr, RemotingCommand request,
            RemotingCommand? response)
        {
        }

        public int CheckCount()
        {
            lock (_lk)
            {
                return _codes.Count(c => c == RequestCode.CheckClientConfig);
            }
        }

        public string CodesStr()
        {
            lock (_lk)
            {
                return Join(_codes.Select(c => c.ToString(CultureInfo.InvariantCulture)));
            }
        }

        public CheckClientRequestBody? FirstBody()
        {
            lock (_lk)
            {
                return _bodies.Count > 0 ? _bodies[0] : null;
            }
        }
    }

    // ---------------- the collecting listener ----------------

    private sealed class Sink
    {
        private readonly object _lk = new();
        private readonly List<string> _bodies = new();
        private readonly List<string> _colors = new();

        public int Count()
        {
            lock (_lk)
            {
                return _bodies.Count;
            }
        }

        public List<string> Sorted()
        {
            lock (_lk)
            {
                var copy = new List<string>(_bodies);
                copy.Sort(StringComparer.Ordinal);
                return copy;
            }
        }

        public SortedSet<string> Colors()
        {
            lock (_lk)
            {
                return new SortedSet<string>(_colors, StringComparer.Ordinal);
            }
        }

        public void Add(MessageExt m)
        {
            lock (_lk)
            {
                _bodies.Add(BodyText(m));
                _colors.Add(m.Properties.TryGetValue("color", out string? v) ? v : "<missing>");
            }
        }
    }

    private sealed class CollectListener : IMessageListenerConcurrently
    {
        private readonly Sink _sink;

        public CollectListener(Sink sink) => _sink = sink;

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            foreach (MessageExt m in msgs)
            {
                _sink.Add(m);
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    // ---------------- environment ----------------

    /// <summary>A topic/group set isolated by Stamp that sweeps itself up when
   /// done.</summary>
    private sealed class Env
    {
        private readonly DefaultMQAdminExt _admin = new("SQL92DOTNETADMIN");
        private readonly List<string> _topics = new();
        private readonly List<string> _groups = new();

        public string Topic()
        {
            string t = "Sql92Dotnet_" + Stamp;
            _topics.Add(t);
            return t;
        }

        public string Group(string kind)
        {
            string g = "GID_Sql92Dotnet_" + Stamp + "_" + kind;
            _groups.Add(g);
            return g;
        }

        public string BrokerAddr()
        {
            try
            {
                TopicRouteData route = _admin.ExamineTopicRoute(MixAll.DefaultTopic);
                if (route.BrokerDatas.Count > 0)
                {
                    string addr = route.BrokerDatas[0].SelectBrokerAddr();
                    if (addr.Length > 0)
                    {
                        return addr;
                    }
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] broker route failed: " + e.Message);
            }

            return "127.0.0.1:10911";
        }

        public void Start()
        {
            _admin.SetNamesrvAddr(_nsAddr);
            _admin.Start();
        }

        /// <summary>
        /// The common consumer wiring: address + starting point + subscription (the TAG
        /// expression when selector is null) + an optional evidence hook.
        /// </summary>
        public void Configure(DefaultMQPushConsumer c, string topic, MessageSelector? selector,
            string expression, IRpcHook? probe)
        {
            c.SetNamesrvAddr(_nsAddr);
            c.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromLastOffset;
            if (probe != null)
            {
                c.SetRpcHook(probe);
            }

            if (selector != null)
            {
                c.Subscribe(topic, selector);
            }
            else
            {
                c.Subscribe(topic, expression);
            }
        }

        public int RouteQueueCount(string topic)
        {
            try
            {
                return _admin.ExamineTopicRoute(topic).QueueDatas.Sum(q => q.ReadQueueNums);
            }
            catch (Exception)
            {
                return 0;
            }
        }

        public void Cleanup()
        {
            string addr = BrokerAddr();
            foreach (string t in _topics.ToList())
            {
                try
                {
                    _admin.DeleteTopic(t);
                }
                catch (Exception e)
                {
                    Console.WriteLine("  [diag] DeleteTopic(" + t + ") failed: " + e.Message);
                }
            }

            foreach (string g in _groups.ToList())
            {
            // %RETRY%/%DLQ% go away together with the subscription group (the broker really deletes the retry topic only when the delete-group endpoint is used)
                try
                {
                    _admin.DeleteSubscriptionGroup(addr, g, true);
                }
                catch (Exception e)
                {
                    Console.WriteLine(
                        "  [diag] DeleteSubscriptionGroup(" + g + ") failed: " + e.Message);
                }

                try
                {
                    _admin.DeleteTopic(MixAll.GetRetryTopic(g));
                }
                catch (Exception)
                {
                // the retry topic may already have been taken down along with the group
                }
            }

            _admin.Shutdown();
        }
    }

    // ---------------- S1 the wire evidence ----------------

    private static void S1WireEvidence(Env env, string topic)
    {
        Console.WriteLine("\n---------- S1 the 46 requests during startup ----------");
        string sqlGroup = env.Group("SQL");
        var probe = new CheckConfigProbe();
        var sqlSink = new Sink();
        var sqlConsumer = new DefaultMQPushConsumer(sqlGroup);
        env.Configure(sqlConsumer, topic, MessageSelector.BySql("color = 'red'"), "", probe);
        sqlConsumer.SetMessageListener(new CollectListener(sqlSink));
        try
        {
            sqlConsumer.Start();
            sqlConsumer.Shutdown();
        }
        catch (Exception e)
        {
  Check("S1 the SQL92 consumer started successfully", false, e.Message);
        }

   Check("S1 the SQL92 subscription triggers exactly one CHECK_CLIENT_CONFIG(46)", probe.CheckCount() == 1,
            "codes=" + probe.CodesStr());
        CheckClientRequestBody? body = probe.FirstBody();
        bool shapeOk = body != null
                       && body.Group == sqlGroup
                       && body.ClientId == sqlConsumer.ClientId
                       && body.SubscriptionData?.ExpressionType == ExpressionType.Sql92
                       && body.SubscriptionData?.SubString == "color = 'red'"
                       && body.SubscriptionData?.Topic == topic;
        Check("S1 the body is a CheckClientRequestBody (clientId / group / subscriptionData)", shapeOk,
            "hasBody=" + (body != null)
            + " group=" + body?.Group
            + " clientId=" + body?.ClientId + " want=" + sqlConsumer.ClientId
            + " type=" + body?.SubscriptionData?.ExpressionType
            + " sub=" + body?.SubscriptionData?.SubString
            + " topic=" + body?.SubscriptionData?.Topic + " want=" + topic);

        string tagGroup = env.Group("TAG");
        var tagProbe = new CheckConfigProbe();
        var tagSink = new Sink();
        var tagConsumer = new DefaultMQPushConsumer(tagGroup);
        env.Configure(tagConsumer, topic, null, "tagA || tagB", tagProbe);
        tagConsumer.SetMessageListener(new CollectListener(tagSink));
        try
        {
            tagConsumer.Start();
        }
        catch (Exception e)
        {
        Check("S1 the TAG consumer started successfully", false, e.Message);
        }

      Check("S1 a pure TAG subscription sends not a single 46 (the isTagType short circuit)", tagProbe.CheckCount() == 0,
            "codes=" + tagProbe.CodesStr());
        tagConsumer.Shutdown();
    }

    // ---------------- S2 / S3 ----------------

    private static void S2S3RealFiltering(Env env, string topic, DefaultMQProducer prod)
    {
        Console.WriteLine("\n---------- S2 the broker filters by property / S3 an expression that never matches ----------");
        var redSink = new Sink();
        var greenSink = new Sink();
        var allSink = new Sink();
        var redConsumer = new DefaultMQPushConsumer(env.Group("FILTER"));
        var greenConsumer = new DefaultMQPushConsumer(env.Group("NONE"));
        var allConsumer = new DefaultMQPushConsumer(env.Group("ALL"));
        env.Configure(redConsumer, topic, MessageSelector.BySql("color = 'red'"), "", null);
        env.Configure(greenConsumer, topic, MessageSelector.BySql("color = 'green'"), "", null);
        env.Configure(allConsumer, topic, null, "*", null);
        redConsumer.SetMessageListener(new CollectListener(redSink));
        greenConsumer.SetMessageListener(new CollectListener(greenSink));
        allConsumer.SetMessageListener(new CollectListener(allSink));
        // It has to be started before the send: a new group's CONSUME_FROM_LAST_OFFSET skips
    // the messages sent before startup.
        redConsumer.Start();
        greenConsumer.Start();
        allConsumer.Start();

        try
        {
            var redSent = new List<string>();
            var blueSent = new List<string>();
            foreach (string color in new[] { "red", "blue" })
            {
                for (int i = 0; i < 3; ++i)
                {
                    string text = "body-" + color + "-" + i.ToString(CultureInfo.InvariantCulture);
                    var msg = new Message(topic, Encoding.UTF8.GetBytes(text))
                    {
                        Keys = Stamp + "-" + color + "-" + i
                            .ToString(CultureInfo.InvariantCulture),
                    };
                    msg.PutProperty("color", color);
                    SendResult r = prod.Send(msg, 5000);
                    Check("S2 sending " + text + " succeeded", r.SendStatus == SendStatus.SendOk, r.MsgId);
                    (color == "red" ? redSent : blueSent).Add(text);
                }
            }

            redSent.Sort(StringComparer.Ordinal);
            blueSent.Sort(StringComparer.Ordinal);

            WaitUntil(() => allSink.Count() >= 6, 25000);
            Thread.Sleep(4000); // wait a bit more, to confirm green is not "it just arrived late"
            List<string> gotRed = redSink.Sorted();
            List<string> gotAll = allSink.Sorted();
            Check("S2 the SQL92(color=red) consumer received 3, and exactly the 3 that were sent",
                gotRed.SequenceEqual(redSent), Join(gotRed));
            List<string> everything = redSent.Concat(blueSent).ToList();
            everything.Sort(StringComparer.Ordinal);
     Check("S2 the TAG '*' control group received all 6 (red+blue)", gotAll.SequenceEqual(everything),
                Join(gotAll));
     Check("S2 the 3 blue ones did not leak into the SQL92 consumer (proving the broker really filters)",
                !blueSent.Any(gotRed.Contains), Join(gotRed));
            SortedSet<string> colors = redSink.Colors();
          Check("S2 the color property of the received messages is readable and all red", colors.SetEquals(new[] { "red" }),
                Join(colors));
      Check("S3 subscribing to the never-matching color='green' => nothing received", greenSink.Count() == 0,
                Join(greenSink.Sorted()));
        }
        finally
        {
            redConsumer.Shutdown();
            greenConsumer.Shutdown();
            allConsumer.Shutdown();
        }
    }

    // ---------------- S4 the counter-proof ----------------

    private static void S4BadExpressionFails(Env env, string topic)
    {
        Console.WriteLine("\n---------- S4 an illegal expression fails at startup ----------");
        string badGroup = env.Group("BAD");
        var sink = new Sink();
        var consumer = new DefaultMQPushConsumer(badGroup);
        env.Configure(consumer, topic, MessageSelector.BySql("color =="), "", null);
        consumer.SetMessageListener(new CollectListener(sink));

        long began = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();
        int threw;
        int code = 0;
        string message = "";
        try
        {
            consumer.Start();
            threw = 0;
        }
        catch (MQClientException e)
        {
            threw = 1;
            code = e.ResponseCode;
            message = e.Message;
        }
        catch (Exception e)
        {
            threw = 1;
            message = "wrong exception type: " + e.Message;
        }

        long costMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() - began;
     Check("S4 Start() threw MQClientException", threw == 1, message);
     Check("S4 the error code is the broker's SUBSCRIPTION_PARSE_FAILED(23)",
            code == ResponseCode.SubscriptionParseFailed,
            "code=" + code.ToString(CultureInfo.InvariantCulture) + " remark=" + message);
        Check("S4 after the failure the consumer did not stay in the started state", !consumer.IsStarted);
        Check("S4 the illegal expression fails at once (<5s, not by waiting out a timeout backstop)", costMs < 5000,
            costMs.ToString(CultureInfo.InvariantCulture) + "ms");

        // Was the rollback clean? The same object with a legal expression must be able to Start again
        string retryMessage = "";
        try
        {
            env.Configure(consumer, topic, MessageSelector.BySql("color = 'blue'"), "", null);
            consumer.Start();
        }
        catch (Exception e)
        {
            retryMessage = e.Message;
        }

   Check("S4 after the failure a corrected expression can Start again (the startup was rolled back in place)", retryMessage.Length == 0,
            retryMessage);
        if (retryMessage.Length == 0)
        {
            consumer.Shutdown();
        }
    }

    public static int Run(string[] args)
    {
        if (args.Length > 0)
        {
            _nsAddr = args[0];
        }

        Console.WriteLine("SQL92 / CHECK_CLIENT_CONFIG live check on " + _nsAddr
                                                   + " (stamp=" + Stamp + ")");

        var env = new Env();
        string topic = env.Topic();
        env.Start();

        var producer = new DefaultMQProducer("GID_Sql92Dotnet_" + Stamp + "_P")
        {
            NamesrvAddr = _nsAddr,
            SendMsgTimeout = 5000,
        };
        producer.Start();
        try
        {
       // 4 queues: explicitly asked for 4 queues when creating the topic
            producer.CreateTopic(MixAll.DefaultTopic, topic, 4);
     Check("T0 the topic route is ready (4 queues)",
                WaitUntil(() => env.RouteQueueCount(topic) >= 4, 20000),
                "queues=" + env.RouteQueueCount(topic).ToString(CultureInfo.InvariantCulture));

            S1WireEvidence(env, topic);
            S2S3RealFiltering(env, topic, producer);
            S4BadExpressionFails(env, topic);
        }
        catch (Exception e)
        {
            Check("aborted", false, e.Message);
        }
        finally
        {
            producer.Shutdown();
            env.Cleanup();
        }

        Console.WriteLine("\n" + _pass.ToString(CultureInfo.InvariantCulture) + " PASS / "
                                                   + _fail.ToString(CultureInfo.InvariantCulture)
                                                   + " FAIL");
        return _fail == 0 ? 0 : 1;
    }
}
