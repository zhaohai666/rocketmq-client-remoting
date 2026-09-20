// Validators / TopicValidator 真机验证（任务 #32）。
// 用法：rmq validators-live [namesrv]
//
// 与 python/verify_validators_live.py、rust/examples/live_validators.rs、
// cpp/examples/validators_live.cpp 同场景对拍：四语言用同一套断言证明
// 「非法名字在本地第一行就被拦掉，合法名字照常在集群收发」。
//
// 前置：NameServer + Broker 已起（普通配置，不需要 ACL/TLS/trace）。
//
// 场景：
//   S1 发送路径：空白/超长/非法字符 topic、禁发的 broker 内部流水、body 档位、
//      INNER_MULTI_DISPATCH 分隔符 —— 全部在 <50ms 内本地拒（namesrv 就在场）
//   S2 批量路径：逐条 CheckMessage（成员非法也要拦）+ 同质性检查
//   S3 生产者 Start()：保留组 / 非法字符 / 超长三道门 + 等长(120)边界放行，失败不进 started 态
//   S4 正腿：合法名字照常在集群收发（push + lite 两路消费者各收到 3 条）
//   S5 对照腿：合法但**不存在**的 topic 要走真往返（broker 自动建出来），比本地拒慢一个数量级
//   S6 pull / lite 的组名门 + 合法 pull 组查队列与位点
//   S7 CreateTopic 的本地拒（空白 / 非法字符 / 系统 topic）
//
// 码值口径（四语言一致）：Java 的 MQClientException(String, Throwable) 用 responseCode=-1
// 表示「纯客户端错误」；本工程（Python 先定、其余照抄）用 SystemError/UNKNOWN=1，
// 只有 CheckMessage 的 body 档位与 LMQ 分隔符带 MessageIllegal(13)。
using System.Diagnostics;
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveValidators
{
    /// <summary>本地校验的超时预算（毫秒）：真往返那一腿是 S5 实测的几十毫秒起步。</summary>
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
    /// 跑一个动作，把抛出的异常按类型摊平成 (是否抛, 文案, 响应码, 耗时)。
    /// 非 MQClientException（如批量的 ArgumentException）码记 0，表示"不在客户端错误码体系里"。
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

    /// <summary>断言「本地就拒」：抛了、文案命中、码值对、且没慢到像跑了网络。返回实测耗时。</summary>
    private static double ExpectLocalReject(string name, Action fn, string needle, int wantCode)
    {
        Captured c = Capture(fn);
        Check(name,
            c.Threw && c.What.Contains(needle, StringComparison.Ordinal) && c.Code == wantCode
            && c.ElapsedMs < LocalBudgetMs,
            (c.Threw ? c.What : "没有抛异常") + "  code=" + c.Code.ToString(CultureInfo.InvariantCulture)
            + "  " + Ms(c.ElapsedMs));
        return c.ElapsedMs;
    }

    /// <summary>断言「Start() 本地就拒，且失败后没有把自己标成 started」。</summary>
    private static double ExpectStartReject(string name, Func<bool> startedFlag, Action startFn,
        string needle)
    {
        Captured c = Capture(startFn);
        Check(name, c.Threw && c.What.Contains(needle, StringComparison.Ordinal) && !startedFlag(),
            (c.Threw ? c.What : "没有抛异常") + "  " + Ms(c.ElapsedMs));
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

    /// <summary>路由查询在没路由时会抛（no route / TOPIC_NOT_EXIST），这里摊平成"队列数 0"。</summary>
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

    // ---------------- S1 发送路径

    private static double S1SendPathRejects(DefaultMQProducer p)
    {
        Console.WriteLine("== S1 发送路径的本地校验（namesrv 在场，仍然不碰网络）==");
        (string Label, string Topic, string Needle, int Code)[] negs =
        {
            ("S1a 空 topic", "", "The specified topic is blank", ResponseCode.SystemError),
            ("S1b 全空白 topic", "   ", "The specified topic is blank", ResponseCode.SystemError),
            ("S1c 超长 topic(128)", RepeatedChar('a', 128), "is longer than topic max length 127",
                ResponseCode.SystemError),
            ("S1d 带点 topic", "Validators.Live", IllegalTail, ResponseCode.SystemError),
            ("S1e 非 ASCII topic", "Validators中文", IllegalTail, ResponseCode.SystemError),
            ("S1f 禁发的 broker 内部流水", "SCHEDULE_TOPIC_XXXX", "is forbidden",
                ResponseCode.SystemError),
        };
        double total = 0.0;
        foreach ((string label, string topic, string needle, int code) in negs)
        {
            string t = topic;
            total += ExpectLocalReject(label, () => p.Send(new Message(t, Bytes("body"))), needle, code);
        }

        double local = total / negs.Length;

        ExpectLocalReject("S1g 空 body",
            () => p.Send(new Message("ValidatorsPositive", Array.Empty<byte>())),
            "the message body length is zero", ResponseCode.MessageIllegal);
        ExpectLocalReject("S1h null message",
            () => Validators.CheckMessage(null, 4 << 20),
            "the message is null", ResponseCode.MessageIllegal);
        ExpectLocalReject("S1i 超 maxMessageSize",
            () => Validators.CheckMessage(new Message("ValidatorsPositive", Bytes("12345")), 4),
            "the message body size over max value, MAX: 4", ResponseCode.MessageIllegal);
        Check("S1j 恰好等于 maxMessageSize 放行",
            !Capture(() => Validators.CheckMessage(
                new Message("ValidatorsPositive", Bytes("1234")), 4)).Threw);

        Message lmq = new("ValidatorsPositive", Bytes("hello"));
        lmq.SetUserProperty(MessageConst.PropertyInnerMultiDispatch, "a" + FileSeparator + "b");
        ExpectLocalReject("S1k INNER_MULTI_DISPATCH 带路径分隔符", () => p.Send(lmq),
            "INNER_MULTI_DISPATCH", ResponseCode.MessageIllegal);
        Message legalLmq = new("ValidatorsPositive", Bytes("hello"));
        legalLmq.SetUserProperty(MessageConst.PropertyInnerMultiDispatch, "%LMQ%queue:testCID");
        Check("S1l 常规 LMQ 取值不误杀",
            !Capture(() => Validators.CheckMessage(legalLmq, 4 << 20)).Threw);

        // 顺序：先 topic → 禁发名单 → body。空 body + 非法 topic 必须报 topic
        Captured order = Capture(() => p.Send(new Message("bad topic", Array.Empty<byte>())));
        Check("S1m 校验顺序：topic 先于 body",
            order.Threw && order.What.Contains("contains illegal characters", StringComparison.Ordinal),
            order.Threw ? order.What : "没有抛异常");
        return local;
    }

    // ---------------- S2 批量

    private static void S2BatchRejects(DefaultMQProducer p, string topic)
    {
        Console.WriteLine("== S2 批量路径的逐条校验 ==");
        ExpectLocalReject("S2a 批量里有一条非法 topic",
            () => p.SendBatch(new List<Message>
            {
                new(topic, Bytes("ok-1")),
                new("bad.topic", Bytes("ok-2")),
            }),
            "The specified topic[bad.topic]" + IllegalTail, ResponseCode.SystemError);

        ExpectLocalReject("S2b 批量里有一条空 body",
            () => p.SendBatch(new List<Message>
            {
                new(topic, Bytes("ok-1")),
                new(topic, Array.Empty<byte>()),
            }),
            "the message body length is zero", ResponseCode.MessageIllegal);

        // 同质性检查在 Java 抛 IllegalArgumentException（不是 MQClientException）—— 码记 0
        Captured mixed = Capture(() => p.SendBatch(new List<Message>
        {
            new(topic, Bytes("ok-1")),
            new(topic + "Other", Bytes("ok-2")),
        }));
        Check("S2c 批量 topic 不同质被拒（非 MQClientException 口径）",
            mixed.Threw && mixed.What.Contains("should be the same", StringComparison.Ordinal)
            && mixed.Code == 0 && mixed.ElapsedMs < LocalBudgetMs,
            mixed.Threw ? mixed.What : "没有抛异常");
    }

    // ---------------- S3 生产者组名门

    private static void S3ProducerGroupGates()
    {
        Console.WriteLine("== S3 生产者 Start() 的组名校验门 ==");
        (string Label, string Group, string Needle)[] negs =
        {
            ("S3a 保留组 DEFAULT_PRODUCER", MixAll.DefaultProducerGroup,
                "producerGroup can not equal DEFAULT_PRODUCER, please specify another one."),
            ("S3b 带空格的组", "bad group", "the specified group[bad group]" + IllegalTail),
            ("S3c 超长组(121)", RepeatedChar('g', 121), "is longer than group max length: 120"),
        };
        foreach ((string label, string group, string needle) in negs)
        {
            // namesrv 指向**真实**集群也照样拒 —— 挡的是本地第一行，与网络可达性无关
            var prod = new DefaultMQProducer(group) { NamesrvAddr = _namesrv };
            ExpectStartReject(label, () => prod.IsStarted, () => prod.Start(), needle);
        }

        // 等长边界：120 字符必须放行（Java 用 >，不是 >=）；这里真起起来再关掉
        var edge = new DefaultMQProducer(RepeatedChar('g', 120)) { NamesrvAddr = _namesrv };
        Captured c = Capture(edge.Start);
        Check("S3d 120 字符组名放行（长度判定是 > 而非 >=）",
            !c.Threw || !c.What.Contains("group max length", StringComparison.Ordinal),
            c.Threw ? c.What : "Start 未因组名长度失败");
        if (edge.IsStarted)
        {
            edge.Shutdown();
        }
    }

    // ---------------- S4 正腿

    private static void S4PositiveLeg(DefaultMQProducer p, string topic, string group, string prefix)
    {
        Console.WriteLine("== S4 正腿：合法名字照常在集群收发 ==");
        var collector = new Collector();
        var cons = new DefaultMQPushConsumer(group);
        cons.SetNamesrvAddr(_namesrv);
        cons.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromLastOffset;
        cons.Subscribe(topic, "*");
        cons.SetMessageListener(collector);
        Captured started = Capture(cons.Start);
        Check("S4a 合法组名的 push 消费者可启动", !started.Threw,
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
        Check("S4b 合法组名的 lite 消费者可启动", !liteStarted.Threw,
            liteStarted.Threw ? liteStarted.What : "ok");

        // 消费者先起来再发消息：新组 + ConsumeFromLastOffset 取的是**启动那一刻**的队尾位点
        bool assigned = WaitUntil(() => lite.Assignment().Count > 0, 20000);
        Check("S4c lite 拿到队列分配", assigned,
            lite.Assignment().Count.ToString(CultureInfo.InvariantCulture) + " 个队列");

        for (int i = 0; i < 3; i++)
        {
            int n = i;
            Captured s = Capture(() => p.Send(new Message(topic, Bytes(prefix + "-body-" + n)), 5000));
            Check("S4d 合法 topic 发送成功 #" + n.ToString(CultureInfo.InvariantCulture), !s.Threw,
                s.Threw ? s.What : "SEND_OK");
        }

        bool gotAll = WaitUntil(() => collector.ByPrefix(prefix).Count >= 3, 30000);
        Check("S4e push 消费者收到全部 3 条", gotAll,
            "收到 " + collector.ByPrefix(prefix).Count.ToString(CultureInfo.InvariantCulture) + " 条");

        int polled = 0;
        long deadline = Stopwatch.GetTimestamp() + 20L * Stopwatch.Frequency;
        while (polled < 3 && Stopwatch.GetTimestamp() < deadline)
        {
            polled += lite.Poll(1000).Count;
        }

        Check("S4f lite 消费者 poll 到全部 3 条", polled >= 3,
            "poll 到 " + polled.ToString(CultureInfo.InvariantCulture) + " 条");
        lite.Shutdown();
        cons.Shutdown();
    }

    // ---------------- S5 对照腿

    private static void S5ControlLeg(DefaultMQProducer p, double localMs)
    {
        Console.WriteLine("== S5 对照腿：本地校验省掉的是什么 ==");
        // 合法但**不存在**的 topic：本地放行 → 真往返（broker 按 TBW102 自动建出来）
        string missing = "ValidatorsMissing" + Stamp;
        Captured c = Capture(() => p.Send(new Message(missing, Bytes("x")), 5000));
        Check("S5a 合法但不存在的 topic 不被本地误伤（broker 自动建出来）",
            !c.Threw || !c.What.Contains(IllegalTail, StringComparison.Ordinal),
            (c.Threw ? c.What : "SEND_OK") + "  " + Ms(c.ElapsedMs));
        double baseline = localMs < 0.05 ? 0.05 : localMs;
        Check("S5b 集群腿比本地反腿慢一个数量级以上", c.ElapsedMs > 10.0 * baseline,
            Ms(c.ElapsedMs) + " vs 本地 " + Ms(baseline));
    }

    // ---------------- S6 消费者侧

    private static void S6ConsumerGates(string topic)
    {
        Console.WriteLine("== S6 pull/lite 的组名门 + 合法 pull 组查位点 ==");
        var pullReserved = new DefaultMQPullConsumer(MixAll.DefaultConsumerGroup);
        pullReserved.SetNamesrvAddr(_namesrv);
        ExpectStartReject("S6a pull 消费者挡 DEFAULT_CONSUMER", () => pullReserved.IsStarted,
            pullReserved.Start,
            "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");

        var liteReserved = new DefaultLitePullConsumer(MixAll.DefaultConsumerGroup);
        liteReserved.SetNamesrvAddr(_namesrv);
        liteReserved.Subscribe(topic, "*");
        ExpectStartReject("S6b lite 消费者挡 DEFAULT_CONSUMER", () => liteReserved.IsStarted,
            liteReserved.Start,
            "consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.");

        var liteIllegal = new DefaultLitePullConsumer("bad group");
        liteIllegal.SetNamesrvAddr(_namesrv);
        liteIllegal.Subscribe(topic, "*");
        ExpectStartReject("S6c lite 消费者挡非法字符组", () => liteIllegal.IsStarted,
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

        Check("S6d 合法组名的 pull 消费者可启动并查到位点",
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
        Console.WriteLine("== S7 CreateTopic 的本地拒 ==");
        ExpectLocalReject("S7a CreateTopic 挡非法字符",
            () => p.CreateTopic(MixAll.DefaultTopic, "bad topic", QueueNums),
            "The specified topic[bad topic]" + IllegalTail, ResponseCode.SystemError);
        ExpectLocalReject("S7b CreateTopic 挡系统 topic",
            () => p.CreateTopic(MixAll.DefaultTopic, "RMQ_SYS_TRACE_TOPIC", QueueNums),
            "is conflict with system topic", ResponseCode.SystemError);
        ExpectLocalReject("S7c CreateTopic 挡空 topic",
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

        Console.WriteLine("=== RocketMQ validators live verify (.NET) ===");
        Console.WriteLine("namesrv=" + _namesrv + " stamp=" + Stamp);

        var producer = new DefaultMQProducer("GID_validators_net_producer_" + Stamp)
        {
            NamesrvAddr = _namesrv,
        };
        Captured started = Capture(producer.Start);
        if (started.Threw)
        {
            Check("S0 生产者启动", false, started.What);
            Report();
            return 1;
        }

        Check("S0 生产者启动", true, "group=" + producer.ProducerGroup);

        double localMs = S1SendPathRejects(producer);
        S2BatchRejects(producer, topic);
        S3ProducerGroupGates();

        Captured created = Capture(() => producer.CreateTopic(MixAll.DefaultTopic, topic, QueueNums));
        if (created.Threw)
        {
            Check("S4 前置：建 topic", false, created.What);
            Report();
            producer.Shutdown();
            return 1;
        }

        bool routed = WaitUntil(
            () => RouteQueueCount(() => producer.FetchPublishMessageQueues(topic)) >= QueueNums,
            20000);
        Check("S4 前置：路由可发现", routed,
            "queues=" + RouteQueueCount(() => producer.FetchPublishMessageQueues(topic)));
        if (routed)
        {
            S4PositiveLeg(producer, topic, group, prefix);
            S6ConsumerGates(topic);
        }

        S5ControlLeg(producer, localMs);
        S7CreateTopicRejects(producer);

        producer.Shutdown();
        Report();
        return _fail == 0 ? 0 : 1;
    }

    private static void Report()
    {
        Console.WriteLine("############ PASS=" + _pass + " FAIL=" + _fail + " ############");
    }
}
