// .NET 客户端 SQL92 过滤 + CHECK_CLIENT_CONFIG(46) 真机联调
// （对应 python/verify_sql92_live.py、cpp/examples/sql92_live.cpp、
//   rust/examples/live_sql92.rs、dotnet/tests/RocketMQ.Client.Tests/CheckClientConfigTests.cs：
//   四语言同一套场景）。
//
// 为什么必须在真集群上跑：SQL92 这条链路最容易「静默失效」。broker 的
// ExpressionMessageFilter 在 ConsumeQueue 阶段拿不到编译好的过滤数据时**直接放行全部
// 消息**（return true），于是两种错都表现为「消费者正常启动、消息也都收到了」：
//   1. broker 没开 enablePropertyFilter ⇒ 表达式根本没被编译；
//   2. 表达式语法错 ⇒ 同上；只有 Java 的 checkClientConfig 会把它变成启动错误。
// 离线单测锁得住协议形状，锁不住 broker 真的按属性过滤了。所以这里四段都验：
//   S1 线上取证：SQL92 订阅 ⇒ 启动时正好一笔 46（body 是 CheckClientRequestBody）；
//      纯 TAG 订阅 ⇒ 一笔都不发（Java ExpressionType.isTagType 短路）
//   S2 真过滤：消费者**先起来再发消息**（新消费组 + CONSUME_FROM_LAST_OFFSET 会跳过启动前
//      的消息，先发消息这一段就是假绿）：SQL92 只订阅 red ⇒ 恰好那 3 条 red；
//      TAG '*' 对照组 ⇒ 6 条全收
//   S3 空结果腿：订阅永不匹配的 color = 'green' ⇒ 一条都不收（排除「其实全放行了」）
//   S4 反证：语法错的表达式让 Start() 抛 SUBSCRIPTION_PARSE_FAILED(23)，且启动就地回滚
//      （同一个对象换成合法表达式能重新 Start）
//
// 前置：本地 5.5.1 集群已起，且 broker 配了 enablePropertyFilter=true。
// 用法：rmq sql92 [namesrv]
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

    // ---------------- 取证钩子 ----------------

    /// <summary>
    /// 钩在 transport 上，抓启动期真正发出去的 46 号请求（含 body）。
    /// DoBeforeRequest 跑在发送线程上、一个钩子对象可能被多线程共用，必须自带锁。
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

    // ---------------- 收集监听器 ----------------

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

    // ---------------- 环境 ----------------

    /// <summary>一套按 Stamp 隔离的 topic/group，跑完自己扫干净。</summary>
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
        /// 消费者通用装配：地址 + 起点 + 订阅（selector 为 null 时用 TAG 表达式）+ 可选取证钩子。
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
                // %RETRY%/%DLQ% 随订阅组一起删（只有走删组接口 broker 才会真删 retry topic）
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
                    // retry topic 可能已被删组接口连带删掉
                }
            }

            _admin.Shutdown();
        }
    }

    // ---------------- S1 线上取证 ----------------

    private static void S1WireEvidence(Env env, string topic)
    {
        Console.WriteLine("\n---------- S1 启动期的 46 号请求 ----------");
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
            Check("S1 SQL92 消费者启动成功", false, e.Message);
        }

        Check("S1 SQL92 订阅触发恰好一笔 CHECK_CLIENT_CONFIG(46)", probe.CheckCount() == 1,
            "codes=" + probe.CodesStr());
        CheckClientRequestBody? body = probe.FirstBody();
        bool shapeOk = body != null
                       && body.Group == sqlGroup
                       && body.ClientId == sqlConsumer.ClientId
                       && body.SubscriptionData?.ExpressionType == ExpressionType.Sql92
                       && body.SubscriptionData?.SubString == "color = 'red'"
                       && body.SubscriptionData?.Topic == topic;
        Check("S1 body 是 CheckClientRequestBody（clientId / group / subscriptionData）", shapeOk,
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
            Check("S1 TAG 消费者启动成功", false, e.Message);
        }

        Check("S1 纯 TAG 订阅一笔 46 都不发（Java isTagType 短路）", tagProbe.CheckCount() == 0,
            "codes=" + tagProbe.CodesStr());
        tagConsumer.Shutdown();
    }

    // ---------------- S2 / S3 ----------------

    private static void S2S3RealFiltering(Env env, string topic, DefaultMQProducer prod)
    {
        Console.WriteLine("\n---------- S2 broker 按属性过滤 / S3 永不匹配的表达式 ----------");
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
        // 必须先起来再发：新消费组的 CONSUME_FROM_LAST_OFFSET 会跳过启动前的消息。
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
                    Check("S2 发送 " + text + " 成功", r.SendStatus == SendStatus.SendOk, r.MsgId);
                    (color == "red" ? redSent : blueSent).Add(text);
                }
            }

            redSent.Sort(StringComparer.Ordinal);
            blueSent.Sort(StringComparer.Ordinal);

            WaitUntil(() => allSink.Count() >= 6, 25000);
            Thread.Sleep(4000); // 再等一会，确认 green 不是"来得晚"
            List<string> gotRed = redSink.Sorted();
            List<string> gotAll = allSink.Sorted();
            Check("S2 SQL92(color=red) 收到 3 条，且正好是发出去的那 3 条",
                gotRed.SequenceEqual(redSent), Join(gotRed));
            List<string> everything = redSent.Concat(blueSent).ToList();
            everything.Sort(StringComparer.Ordinal);
            Check("S2 TAG '*' 对照组收到 6 条（红+蓝全在）", gotAll.SequenceEqual(everything),
                Join(gotAll));
            Check("S2 blue 的 3 条没漏进 SQL92 消费者（证明 broker 真在过滤）",
                !blueSent.Any(gotRed.Contains), Join(gotRed));
            SortedSet<string> colors = redSink.Colors();
            Check("S2 收到的消息属性 color 可读且都是 red", colors.SetEquals(new[] { "red" }),
                Join(colors));
            Check("S3 订阅永不匹配的 color='green' ⇒ 一条都没收到", greenSink.Count() == 0,
                Join(greenSink.Sorted()));
        }
        finally
        {
            redConsumer.Shutdown();
            greenConsumer.Shutdown();
            allConsumer.Shutdown();
        }
    }

    // ---------------- S4 反证 ----------------

    private static void S4BadExpressionFails(Env env, string topic)
    {
        Console.WriteLine("\n---------- S4 非法表达式在启动期失败 ----------");
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
        Check("S4 Start() 抛出 MQClientException", threw == 1, message);
        Check("S4 错误码是 broker 的 SUBSCRIPTION_PARSE_FAILED(23)",
            code == ResponseCode.SubscriptionParseFailed,
            "code=" + code.ToString(CultureInfo.InvariantCulture) + " remark=" + message);
        Check("S4 失败后消费者没留在已启动状态", !consumer.IsStarted);
        Check("S4 非法表达式立刻失败（<5s，不是等超时兜底）", costMs < 5000,
            costMs.ToString(CultureInfo.InvariantCulture) + "ms");

        // 回滚干净了？同一个对象换个合法表达式应当能重新 Start
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

        Check("S4 失败后修正表达式可重新 Start（启动已就地回滚）", retryMessage.Length == 0,
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
            // 队列数 4：与 python/verify_sql92_live.py 的 create_topic("TBW102", topic, 4) 对齐
            producer.CreateTopic(MixAll.DefaultTopic, topic, 4);
            Check("T0 topic 路由就绪（4 队列）",
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
