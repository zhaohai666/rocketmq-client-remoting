// 轻量拉取消费者（DefaultLitePullConsumer）真机验证。
// 用法：rmq lite-pull [namesrv]
//
// 与 cpp/examples/live_lite_pull.cpp、python/verify_lite_pull_live.py 完全同场景
// （三语言对拍用同一套断言）。
//
// 场景（围绕 Lite 相对 Pull 的本质区别：调用方不用管位点，poll 从本地缓冲拿消息）：
//   S1 建 topic + subscribe 模式等待 rebalance 分到位点（4/4 队列）
//   S2 先起消费者、再发 12 条（交替 TagA/TagB）→ subscribe + poll 收全 12 条且内容一致
//   S3 auto-commit：消费后 committed 位点 > 0，且 commit() 后可回读
//   S4 assign 模式：显式 assign 全部队列 + seek 到队首 → poll 重新收全 12 条
//   S5 订阅 TagA：Subscribe(T, "TagA") 只收 TagA 的 6 条（订阅级 tag 过滤）
//   S6 CONSUME_FROM_TIMESTAMP：consumeTimestamp 按 Java 的 14 位本地墙钟解释
//      S6a 新组 + 起点=30 分钟前 → 收全 12 条
//      S6b 墙钟→队列位置映射：30 分钟前 → 各队列队首（Σ=0）；10 分钟后 → 越过全部消息（Σ=12）
//   S7 可插拔分配策略（对应 Java setAllocateMessageQueueStrategy）
//      S7a 默认策略名 AVG；置 null 由 Start() 按 Java checkConfig 拒绝
//      S7b AVG_BY_CIRCLE：同组两实例把队列按下标取模交叉切开，不重不漏
//      S7c CONFIG：只分配配置进去的队列 → assignment 恰为其一，且 Poll 到的消息
//          全部来自配置队列、两半合起来覆盖全部 12 条且互不重叠
//      S7d CONSISTENT_HASH：拿**真实 clientId** 建环，线上 assignment 必须收敛到
//          「真实 mqAll/cidAll 离线跑同一策略」的预测；环可能一边 4 条一边 0 条，
//          所以判定只看「不重不漏 + 等于预测」
//      S7e MACHINE_ROOM_NEARBY：真实集群只有一个机房 ⇒ 装饰器必须原样透传内层策略；
//          resolver 的调用记录同时证明 rebalance 真的逐个问过队列/客户端的机房
//      S7f MACHINE_ROOM：真实 brokerName 不含 '@'，白名单再怎么写都筛不出队列 ——
//          验的是「配错机房安静饿死」（分不到队列、poll 不到消息、不打崩重平衡）
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveLitePull
{
    private static readonly long WallNowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static readonly string Stamp = WallNowMs.ToString(CultureInfo.InvariantCulture);
    private static readonly string Topic = "LiteLiveNet_" + Stamp;

    private const int NMsg = 12;
    private const int QueueNum = 4;

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

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static string BodyOf(MessageExt m) => Encoding.UTF8.GetString(m.Body);

    private static string Join(IEnumerable<string> items) => string.Join(" ", items);

    private static List<MessageQueue> WaitAssignment(DefaultLitePullConsumer c, int timeoutMs = 20000)
    {
        long deadline = NowMs() + timeoutMs;
        while (NowMs() < deadline)
        {
            List<MessageQueue> a = c.Assignment();
            if (a.Count > 0)
            {
                return a;
            }

            Thread.Sleep(500);
        }

        return new List<MessageQueue>();
    }

    private static List<MessageExt> Drain(DefaultLitePullConsumer c, int expect, int timeoutMs = 30000)
    {
        var collected = new List<MessageExt>();
        long deadline = NowMs() + timeoutMs;
        while (collected.Count < expect && NowMs() < deadline)
        {
            collected.AddRange(c.Poll(1000));
        }

        return collected;
    }

    /// <summary>按**时间**排空缓冲：策略只分到部分队列时收条数未知，不能按条数等。</summary>
    private static List<MessageExt> DrainFor(DefaultLitePullConsumer c, int durationMs)
    {
        var collected = new List<MessageExt>();
        long deadline = NowMs() + durationMs;
        while (NowMs() < deadline)
        {
            collected.AddRange(c.Poll(1000));
        }

        return collected;
    }

    /// <summary>队列列表 → key 集合（"brokerName#queueId"），用于跨语言比分配结果。</summary>
    private static HashSet<string> QueueKeys(IEnumerable<MessageQueue> queues)
    {
        var keys = new HashSet<string>(StringComparer.Ordinal);
        foreach (MessageQueue mq in queues)
        {
            keys.Add(mq.BrokerName + "#" + mq.QueueId.ToString(CultureInfo.InvariantCulture));
        }

        return keys;
    }

    private static string KeySetText(IEnumerable<string> keys) =>
        Join(keys.OrderBy(k => k, StringComparer.Ordinal));

    /// <summary>消息列表 → body 集合；给定 onlyOnQueues 时只统计来自这些队列的消息。</summary>
    private static HashSet<string> BodiesOf(IEnumerable<MessageExt> msgs,
        HashSet<string>? onlyOnQueues = null)
    {
        var bodies = new HashSet<string>(StringComparer.Ordinal);
        foreach (MessageExt m in msgs)
        {
            if (onlyOnQueues is null
                || onlyOnQueues.Contains(m.BrokerName + "#"
                    + m.QueueId.ToString(CultureInfo.InvariantCulture)))
            {
                bodies.Add(BodyOf(m));
            }
        }

        return bodies;
    }

    /// <summary>等两个实例把队列**分完**（分配收敛要两边各跑一轮心跳 + rebalance）。</summary>
    private static (List<MessageQueue> A, List<MessageQueue> B) WaitSplitAssignment(
        DefaultLitePullConsumer a, DefaultLitePullConsumer b, int total, int timeoutMs = 25000)
    {
        long deadline = NowMs() + timeoutMs;
        List<MessageQueue> va = new();
        List<MessageQueue> vb = new();
        while (NowMs() < deadline)
        {
            va = a.Assignment();
            vb = b.Assignment();
            HashSet<string> ka = QueueKeys(va), kb = QueueKeys(vb);
            if (ka.Count > 0 && kb.Count > 0 && !ka.Overlaps(kb)
                && new HashSet<string>(ka.Union(kb)).Count == total)
            {
                return (va, vb);
            }

            Thread.Sleep(500);
        }

        return (va, vb);
    }

    /// <summary>给 lite 消费者装上策略 + 订阅（未 Start）。S7d~S7f 三个场景都用同一套配置面。</summary>
    private static void SetupConsumer(DefaultLitePullConsumer c, string namesrv, string instanceName,
        IAllocateMessageQueueStrategy strategy)
    {
        c.SetNamesrvAddr(namesrv);
        c.SetInstanceName(instanceName);
        c.SetPollTimeoutMillis(1000);
        // 存量消息在 S2 就发完了，新组默认 LAST 会跳过它们 → 收不到任何一条。
        // 这里要验的是「策略真的驱动了收发」，所以从队首起消费。
        c.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        c.SetAllocateMessageQueueStrategy(strategy);
        c.Subscribe(Topic, "*");
    }

    private static string SnapshotText(IEnumerable<DefaultLitePullConsumer> asserted,
        IEnumerable<HashSet<string>> live, IEnumerable<HashSet<string>> expected, List<string> cidAll)
    {
        var parts = new List<string>();
        using IEnumerator<DefaultLitePullConsumer> cs = asserted.GetEnumerator();
        using IEnumerator<HashSet<string>> ls = live.GetEnumerator();
        using IEnumerator<HashSet<string>> es = expected.GetEnumerator();
        while (cs.MoveNext() && ls.MoveNext() && es.MoveNext())
        {
            parts.Add("cid=" + cs.Current.ClientId + " live=[" + KeySetText(ls.Current)
                + "] predict=[" + KeySetText(es.Current) + "]");
        }

        parts.Add("cidAll=[" + Join(cidAll) + "]");
        return string.Join(" | ", parts);
    }

    /// <summary>
    /// 用**真实**输入（路由给的 mqAll + cidMembers 的真实 clientId 当 cidAll）离线跑策略，
    /// 并**等线上 Assignment() 收敛到这份预测**。
    /// </summary>
    /// <remarks>
    /// 为什么以预测为收敛条件，而不是「先等不重不漏、再比预测」：哈希环完全可能把 4 个
    /// 队列全分给一个实例，另一边在首轮重平衡之前 Assignment() 本来就是空 —— 那种初始态
    /// 同样满足「不重不漏」，比出来的其实是「一边还没算」的快照。（Rust 版第一版就在这里翻车。）
    /// Java RebalanceImpl#rebalanceByTopic 调策略前会把 mqAll、cidAll 都 Collections.sort，
    /// 所以这里也得自己排：mqAll 由调用方排好，clientId 按 Ordinal 序（同 String#compareTo）。
    /// strategies 与 asserted 一一对应 —— S7f 要故意让两边配不同策略。
    /// </remarks>
    private static (bool Converged, string Detail) WaitUntilPredictionConverged(
        string group, List<MessageQueue> mqAll, IEnumerable<DefaultLitePullConsumer> cidMembers,
        IReadOnlyList<DefaultLitePullConsumer> asserted, IReadOnlyList<IAllocateMessageQueueStrategy> strategies,
        int timeoutMs = 45000)
    {
        List<string> cidAll = cidMembers.Select(c => c.ClientId)
            .OrderBy(s => s, StringComparer.Ordinal).ToList();
        var expected = new List<HashSet<string>>();
        for (int i = 0; i < asserted.Count; ++i)
        {
            expected.Add(QueueKeys(strategies[i].Allocate(group, asserted[i].ClientId, mqAll, cidAll)));
        }

        long deadline = NowMs() + timeoutMs;
        while (true)
        {
            List<HashSet<string>> live = asserted.Select(c => QueueKeys(c.Assignment())).ToList();
            bool matched = live.Count == expected.Count;
            for (int i = 0; matched && i < live.Count; ++i)
            {
                matched = live[i].SetEquals(expected[i]);
            }

            if (matched || NowMs() >= deadline)
            {
                return (matched, SnapshotText(asserted, live, expected, cidAll));
            }

            Thread.Sleep(500);
        }
    }

    /// <summary>
    /// 「全员同机房」resolver：真实集群只有一个 broker，把队列和客户端都记成同一个机房，
    /// 于是 NEARBY 必定走「自己机房」那条分支、等价于内层策略。同时留调用记录 ——
    /// 证明 rebalance 真的逐个问过队列和客户端的机房，而不是策略换了个名字却没参与分配。
    /// </summary>
    private sealed class OneRoom : IMachineRoomResolver
    {
        public const string Room = "room1";

        private readonly object _gate = new();
        private readonly List<string> _brokerCalls = new();
        private readonly List<string> _consumerCalls = new();

        public string BrokerDeployIn(MessageQueue messageQueue)
        {
            lock (_gate)
            {
                _brokerCalls.Add(messageQueue.BrokerName);
            }

            return Room;
        }

        public string ConsumerDeployIn(string clientId)
        {
            lock (_gate)
            {
                _consumerCalls.Add(clientId);
            }

            return Room;
        }

        public List<string> BrokerCalls()
        {
            lock (_gate)
            {
                return new List<string>(_brokerCalls);
            }
        }

        public List<string> ConsumerCalls()
        {
            lock (_gate)
            {
                return new List<string>(_consumerCalls);
            }
        }
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        string group1 = "LitePG1Net_" + Stamp;
        string group2 = "LitePG2Net_" + Stamp;
        string group3 = "LitePG3Net_" + Stamp;
        string group4 = "LitePG4Net_" + Stamp;
        string group5 = "LitePG5Net_" + Stamp;
        string group6 = "LitePG6Net_" + Stamp;
        string group7 = "LitePG7Net_" + Stamp;
        string group8 = "LitePG8Net_" + Stamp;
        string group9 = "LitePG9Net_" + Stamp;
        string group10 = "LitePG10Net_" + Stamp;   // S7d 一致性哈希环
        string group11 = "LitePG11Net_" + Stamp;   // S7e 机房就近
        string group12 = "LitePG12Net_" + Stamp;   // S7f 机房配错

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("LitePullConsumer live (.NET): namesrv=" + namesrv + " topic=" + Topic
            + " group=" + group1);
        Console.WriteLine(new string('=', 70));

        // ---------------- 建 topic ----------------
        {
            var prep = new DefaultMQProducer("PG_PrepareLiteNet_" + Stamp);
            prep.NamesrvAddr = namesrv;
            prep.Start();
            try
            {
                prep.CreateTopic("TBW102", Topic, QueueNum);
            }
            catch (Exception e)
            {
                Console.WriteLine("!! CreateTopic failed: " + e.Message);
            }

            prep.Shutdown();
        }

        // ---------------- S1 subscribe 模式：先起消费者再发消息 ----------------
        Console.WriteLine();
        Console.WriteLine("S1 subscribe 模式启动 + 等待 rebalance 分到位点");
        var c1 = new DefaultLitePullConsumer(group1);
        c1.SetNamesrvAddr(namesrv);
        c1.SetPollTimeoutMillis(1000);
        c1.Subscribe(Topic, "*");
        c1.Start();

        List<MessageQueue> assigned = WaitAssignment(c1);
        Check("S1 rebalance 分配到 " + QueueNum.ToString(CultureInfo.InvariantCulture) + " 个队列",
            assigned.Count == QueueNum,
            "assigned=" + assigned.Count.ToString(CultureInfo.InvariantCulture));
        if (assigned.Count == 0)
        {
            Console.WriteLine("!! 未分配到队列，后续跳过");
            c1.Shutdown();
            Console.WriteLine();
            Console.WriteLine("LitePullConsumer: PASS=" + _pass + " FAIL=" + _fail);
            return 1;
        }

        // ---------------- S2 生产 + poll 收全 ----------------
        Console.WriteLine();
        Console.WriteLine("S2 生产 " + NMsg.ToString(CultureInfo.InvariantCulture) + " 条（交替 TagA/TagB）");
        var sent = new HashSet<string>(StringComparer.Ordinal);
        {
            var prod = new DefaultMQProducer("PG_LiteLiveNet_" + Stamp);
            prod.NamesrvAddr = namesrv;
            prod.Start();
            int sentOk = 0;
            for (int i = 0; i < NMsg; ++i)
            {
                string body = "lite-" + i.ToString("D2", CultureInfo.InvariantCulture);
                try
                {
                    var msg = new Message(Topic, Encoding.UTF8.GetBytes(body));
                    msg.Keys = "lite-key-" + body;
                    msg.Tags = i % 2 == 0 ? "TagA" : "TagB";
                    SendResult r = prod.Send(msg);
                    if (r.SendStatus == SendStatus.SendOk)
                    {
                        ++sentOk;
                        sent.Add(body);
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("   send " + i.ToString(CultureInfo.InvariantCulture)
                        + " failed: " + e.Message);
                }
            }

            Check("S2 生产 " + NMsg.ToString(CultureInfo.InvariantCulture) + " 条成功", sentOk == NMsg,
                "sentOk=" + sentOk.ToString(CultureInfo.InvariantCulture));
            prod.Shutdown();
        }

        List<MessageExt> got = Drain(c1, NMsg);
        {
            var gotSet = new HashSet<string>(StringComparer.Ordinal);
            foreach (MessageExt m in got)
            {
                gotSet.Add(BodyOf(m));
            }

            var missing = new HashSet<string>(sent.Except(gotSet));
            var extra = new HashSet<string>(gotSet.Except(sent));
            Check("S2 subscribe+poll 收全 "
                + NMsg.ToString(CultureInfo.InvariantCulture) + " 条且内容一致", gotSet.SetEquals(sent),
                "got=" + gotSet.Count.ToString(CultureInfo.InvariantCulture)
                + " missing=[" + Join(missing) + "] extra=[" + Join(extra) + "]");
        }

        // ---------------- S3 auto-commit 位点 ----------------
        Console.WriteLine();
        Console.WriteLine("S3 auto-commit 位点");
        {
            bool allPositive = true;
            var detail = new StringBuilder();
            foreach (MessageQueue mq in assigned)
            {
                long v = c1.Committed(mq);
                detail.Append(v.ToString(CultureInfo.InvariantCulture)).Append(' ');
                if (v <= 0)
                {
                    allPositive = false;
                }
            }

            Check("S3 各队列 committed 位点 > 0", allPositive, "committed=" + detail);
        }

        // ---------------- S4 assign 模式：assign 全部队列 + seek 到队首重新收全 ----------------
        Console.WriteLine();
        Console.WriteLine("S4 assign 模式：assign 全部队列 + seek 到队首重新收全");
        {
            var c2 = new DefaultLitePullConsumer(group2);
            c2.SetNamesrvAddr(namesrv);
            c2.SetPollTimeoutMillis(1000);
            c2.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
            List<MessageQueue> allQueues = c1.FetchMessageQueues(Topic);
            c2.Assign(allQueues);
            c2.Start();
            foreach (MessageQueue mq in allQueues)
            {
                c2.SeekToBegin(mq);
            }

            List<MessageExt> got2 = Drain(c2, NMsg);
            var got2Set = new HashSet<string>(StringComparer.Ordinal);
            foreach (MessageExt m in got2)
            {
                got2Set.Add(BodyOf(m));
            }

            Check("S4 assign+seek+poll 重新收全 "
                + NMsg.ToString(CultureInfo.InvariantCulture) + " 条", got2Set.SetEquals(sent),
                "got=" + got2Set.Count.ToString(CultureInfo.InvariantCulture));
            c2.Shutdown();
        }

        // ---------------- S5 订阅级 tag 过滤 ----------------
        Console.WriteLine();
        Console.WriteLine("S5 订阅 TagA：只收 TagA 的 6 条");
        {
            var c3 = new DefaultLitePullConsumer(group3);
            c3.SetNamesrvAddr(namesrv);
            c3.SetPollTimeoutMillis(1000);
            c3.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
            c3.Subscribe(Topic, "TagA");
            c3.Start();
            WaitAssignment(c3);
            List<MessageExt> got3 = Drain(c3, 6);
            var got3Set = new HashSet<string>(StringComparer.Ordinal);
            foreach (MessageExt m in got3)
            {
                got3Set.Add(BodyOf(m));
            }

            // 偶数下标（lite-00/02/...）才是 TagA
            bool onlyA = got3Set.Count == 6
                && got3Set.All(b => (b[^1] - '0') % 2 == 0);
            Check("S5 仅收 TagA 且恰好 6 条", onlyA,
                "got=" + got3Set.Count.ToString(CultureInfo.InvariantCulture));
            c3.Shutdown();
        }

        // ---------------- S6 CONSUME_FROM_TIMESTAMP：14 位本地墙钟 ----------------
        Console.WriteLine();
        Console.WriteLine("S6 CONSUME_FROM_TIMESTAMP：墙钟起点收全 + 时间戳→位点映射");
        long wall = NowMs();
        {
            var c4 = new DefaultLitePullConsumer(group4);
            c4.SetNamesrvAddr(namesrv);
            c4.SetPollTimeoutMillis(1000);
            c4.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromTimestamp);
            c4.SetConsumeTimestamp(UtilAll.TimeMillisToHumanString3(wall - 30 * 60 * 1000));
            c4.Subscribe(Topic, "*");
            c4.Start();
            WaitAssignment(c4);
            List<MessageExt> got4 = Drain(c4, NMsg);
            var got4Set = new HashSet<string>(StringComparer.Ordinal);
            foreach (MessageExt m in got4)
            {
                got4Set.Add(BodyOf(m));
            }

            Check("S6a 起点=" + c4.ConsumeTimestamp + " 早于全部消息 → 收全 "
                + NMsg.ToString(CultureInfo.InvariantCulture) + " 条", got4Set.SetEquals(sent),
                "got=" + got4Set.Count.ToString(CultureInfo.InvariantCulture));
            c4.Shutdown();
        }

        {
            var c5 = new DefaultLitePullConsumer(group5);
            c5.SetNamesrvAddr(namesrv);
            c5.SetPollTimeoutMillis(1000);
            c5.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromTimestamp);
            c5.SetConsumeTimestamp(UtilAll.TimeMillisToHumanString3(wall + 10 * 60 * 1000));
            c5.Subscribe(Topic, "*");
            c5.Start();
            List<MessageQueue> q5 = WaitAssignment(c5);
            // 不断言「未来时间戳收不到消息」：Java 的 RebalanceLitePullImpl 先读已提交位点，
            // 新组只要队首仍在 commitlog 内，broker 就直接回 0，consume_from_where 不参与。
            // 墙钟真正影响的是「时间戳 → 队列位置」的映射，所以断言这个量。
            long sumPast = q5.Sum(mq => c5.OffsetForTimestamp(mq, wall - 30 * 60 * 1000));
            long sumFuture = q5.Sum(mq => c5.OffsetForTimestamp(mq, wall + 10 * 60 * 1000));
            Check("S6b 30 分钟前 → 各队列队首", sumPast == 0, "sumOffset=" + sumPast);
            Check("S6b 10 分钟后 → 越过全部 " + NMsg.ToString(CultureInfo.InvariantCulture) + " 条",
                sumFuture == NMsg, "sumOffset=" + sumFuture);
            c5.Shutdown();
        }

        // ---------------- S7 可插拔队列分配策略 ----------------
        Console.WriteLine();
        Console.WriteLine("S7 可插拔队列分配策略（对应 Java setAllocateMessageQueueStrategy）");
        {
            List<MessageQueue> allQueues = c1.FetchMessageQueues(Topic);
            allQueues.Sort();
            int totalQueues = allQueues.Count;

            // S7a 默认策略 / null 守卫（Java 在 checkConfig 里拒绝 null）
            var probe = new DefaultLitePullConsumer(group6);
            probe.SetNamesrvAddr(namesrv);
            Check("S7a 默认策略名 = AVG",
                probe.AllocateMessageQueueStrategy?.GetName() == "AVG",
                "name=" + (probe.AllocateMessageQueueStrategy?.GetName() ?? "<null>"));
            probe.SetAllocateMessageQueueStrategy(null);
            probe.Subscribe(Topic, "*");
            try
            {
                probe.Start();
                Check("S7a 策略为 null 时 Start() 报 Java 同款文案", false, "没有抛异常");
                probe.Shutdown();
            }
            catch (MQClientException e)
            {
                Check("S7a 策略为 null 时 Start() 报 Java 同款文案",
                    e.Message.Contains("allocateMessageQueueStrategy is null", StringComparison.Ordinal),
                    e.Message);
            }

            // S7b AVG_BY_CIRCLE：同组两实例交叉切分，不重不漏
            var circle = new AllocateMessageQueueAveragelyByCircle();
            var ca = new DefaultLitePullConsumer(group7);
            var cb = new DefaultLitePullConsumer(group7);
            // 同进程两个实例必须有不同的 instanceName（clientId 前缀），否则 broker 侧只看到一个消费者
            ca.SetInstanceName("s7ca");
            cb.SetInstanceName("s7cb");
            foreach (DefaultLitePullConsumer c in new[] { ca, cb })
            {
                c.SetNamesrvAddr(namesrv);
                c.SetPollTimeoutMillis(1000);
                c.SetAllocateMessageQueueStrategy(circle);
                c.Subscribe(Topic, "*");
            }

            Check("S7b 替换后策略名 = AVG_BY_CIRCLE",
                ca.AllocateMessageQueueStrategy?.GetName() == "AVG_BY_CIRCLE");
            ca.Start();
            cb.Start();
            (List<MessageQueue> qa, List<MessageQueue> qb) = WaitSplitAssignment(ca, cb, totalQueues);
            HashSet<string> ka = QueueKeys(qa), kb = QueueKeys(qb);
            var overlap = new HashSet<string>(ka.Intersect(kb, StringComparer.Ordinal));
            var both = new HashSet<string>(ka.Union(kb, StringComparer.Ordinal));
            Check("S7b 两实例分配无交集", overlap.Count == 0, "overlap=[" + KeySetText(overlap) + "]");
            Check("S7b 并集覆盖全部 " + totalQueues.ToString(CultureInfo.InvariantCulture) + " 个队列",
                both.SetEquals(QueueKeys(allQueues)),
                "a=" + ka.Count.ToString(CultureInfo.InvariantCulture)
                + " b=" + kb.Count.ToString(CultureInfo.InvariantCulture));
            // 环形分配的签名：拿到的是「按下标取模」的交叉队列而非连续段
            // （4 队列 / 2 实例 → 各 2 条且下标步长为 2；AVG 会给连续两段）。
            var posA = new List<int>();
            for (int i = 0; i < allQueues.Count; ++i)
            {
                if (ka.Contains(allQueues[i].BrokerName + "#"
                        + allQueues[i].QueueId.ToString(CultureInfo.InvariantCulture)))
                {
                    posA.Add(i);
                }
            }

            bool circleShape = posA.Count == 2;
            for (int i = 1; i < posA.Count; ++i)
            {
                if ((posA[i] - posA[i - 1]) % 2 != 0)
                {
                    circleShape = false;
                }
            }

            Check("S7b 分配形状是交叉（步长 2），不是 AVG 的连续段",
                totalQueues != 4 || circleShape,
                "posA=[" + Join(posA.Select(x => x.ToString(CultureInfo.InvariantCulture))) + "]");
            ca.Shutdown();
            cb.Shutdown();

            // S7c CONFIG：只分配配置进去的一半队列，Poll 到的消息也只能来自这些队列
            List<MessageQueue> halfA = allQueues.GetRange(0, totalQueues / 2);
            List<MessageQueue> halfB = allQueues.GetRange(totalQueues / 2, totalQueues - totalQueues / 2);
            var cfgA = new AllocateMessageQueueByConfig(halfA);
            var cfgB = new AllocateMessageQueueByConfig(halfB);
            var c6 = new DefaultLitePullConsumer(group8);
            var c7 = new DefaultLitePullConsumer(group9);
            foreach ((DefaultLitePullConsumer c, IAllocateMessageQueueStrategy s) in
                     new (DefaultLitePullConsumer, IAllocateMessageQueueStrategy)[] { (c6, cfgA), (c7, cfgB) })
            {
                c.SetNamesrvAddr(namesrv);
                c.SetPollTimeoutMillis(1000);
                c.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
                c.SetAllocateMessageQueueStrategy(s);
                c.Subscribe(Topic, "*");
            }

            c6.Start();
            c7.Start();
            List<MessageQueue> a6 = WaitAssignment(c6);
            List<MessageQueue> a7 = WaitAssignment(c7);
            Check("S7c CONFIG 只给配置进去的队列（无视 mqAll/cidAll）",
                QueueKeys(a6).SetEquals(QueueKeys(halfA)) && QueueKeys(a7).SetEquals(QueueKeys(halfB)),
                "a=[" + KeySetText(QueueKeys(a6)) + "] b=[" + KeySetText(QueueKeys(a7)) + "]");
            // 两个 CONFIG 消费者各看一半：并集恰好是全部 12 条、交集为空 → 策略真的驱动了收发
            List<MessageExt> g6 = DrainFor(c6, 8000);
            List<MessageExt> g7 = DrainFor(c7, 8000);
            HashSet<string> cfgKeysA = QueueKeys(halfA), cfgKeysB = QueueKeys(halfB);
            HashSet<string> b6 = BodiesOf(g6), b7 = BodiesOf(g7);
            HashSet<string> onCfgA = BodiesOf(g6, cfgKeysA), onCfgB = BodiesOf(g7, cfgKeysB);
            Check("S7c CONFIG 消费者只收到自己配置队列里的消息",
                b6.SetEquals(onCfgA) && b7.SetEquals(onCfgB),
                "a=" + b6.Count.ToString(CultureInfo.InvariantCulture)
                + " aOnCfg=" + onCfgA.Count.ToString(CultureInfo.InvariantCulture)
                + " b=" + b7.Count.ToString(CultureInfo.InvariantCulture)
                + " bOnCfg=" + onCfgB.Count.ToString(CultureInfo.InvariantCulture));
            var unionBodies = new HashSet<string>(b6.Union(b7, StringComparer.Ordinal));
            var interBodies = new HashSet<string>(b6.Intersect(b7, StringComparer.Ordinal));
            Check("S7c 两半合起来恰好覆盖全部 "
                + NMsg.ToString(CultureInfo.InvariantCulture) + " 条且互不重叠",
                unionBodies.SetEquals(sent) && interBodies.Count == 0,
                "union=" + unionBodies.Count.ToString(CultureInfo.InvariantCulture)
                + " inter=" + interBodies.Count.ToString(CultureInfo.InvariantCulture));
            c6.Shutdown();
            c7.Shutdown();

            // S7d CONSISTENT_HASH：用**真实 clientId** 建环，线上分配要收敛到离线预测。
            // 判定只看「不重不漏」：环的落点由 clientId 的 MD5 决定，真实集群上完全可能
            // 一边 4 条、另一边 0 条（Java 同款偏斜），所以不能要求两边都非空。
            {
                var ch = new AllocateMessageQueueConsistentHash();
                var d1 = new DefaultLitePullConsumer(group10);
                var d2 = new DefaultLitePullConsumer(group10);
                SetupConsumer(d1, namesrv, "s7da", ch);
                SetupConsumer(d2, namesrv, "s7db", ch);
                Check("S7d 替换后策略名 = CONSISTENT_HASH",
                    d1.AllocateMessageQueueStrategy?.GetName() == "CONSISTENT_HASH",
                    d1.AllocateMessageQueueStrategy?.GetName() ?? "<null>");
                d1.Start();
                d2.Start();
                (bool converged, string detail) = WaitUntilPredictionConverged(
                    group10, allQueues, new[] { d1, d2 }, new[] { d1, d2 }, new IAllocateMessageQueueStrategy[] { ch, ch });
                Check("S7d 线上分配收敛到一致性环的离线预测", converged, detail);
                HashSet<string> k1 = QueueKeys(d1.Assignment()), k2 = QueueKeys(d2.Assignment());
                Check("S7d 两实例分配无交集", !k1.Overlaps(k2),
                    "overlap=[" + KeySetText(k1.Intersect(k2, StringComparer.Ordinal)) + "]");
                Check("S7d 并集覆盖全部 " + totalQueues.ToString(CultureInfo.InvariantCulture) + " 个队列",
                    new HashSet<string>(k1.Union(k2, StringComparer.Ordinal)).SetEquals(QueueKeys(allQueues)),
                    "a=" + k1.Count.ToString(CultureInfo.InvariantCulture)
                    + " b=" + k2.Count.ToString(CultureInfo.InvariantCulture));
                HashSet<string> joined = BodiesOf(DrainFor(d1, 8000));
                joined.UnionWith(BodiesOf(DrainFor(d2, 8000)));
                Check("S7d 两实例合起来收到全部 "
                    + NMsg.ToString(CultureInfo.InvariantCulture) + " 条（环真的在驱动收发）",
                    joined.SetEquals(sent), "union=" + joined.Count.ToString(CultureInfo.InvariantCulture));
                d1.Shutdown();
                d2.Shutdown();
            }

            // S7e MACHINE_ROOM_NEARBY：真实集群只有一个机房 ⇒ 装饰器必须原样透传内层策略。
            {
                var inner = new AllocateMessageQueueConsistentHash();
                var resolver = new OneRoom();
                var nearby = new AllocateMachineRoomNearby(inner, resolver);
                var e1 = new DefaultLitePullConsumer(group11);
                var e2 = new DefaultLitePullConsumer(group11);
                SetupConsumer(e1, namesrv, "s7ea", nearby);
                SetupConsumer(e2, namesrv, "s7eb", nearby);
                Check("S7e 装饰后的策略名 = MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
                    e1.AllocateMessageQueueStrategy?.GetName() == "MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
                    e1.AllocateMessageQueueStrategy?.GetName() ?? "<null>");
                e1.Start();
                e2.Start();
                (bool converged, string detail) = WaitUntilPredictionConverged(
                    group11, allQueues, new[] { e1, e2 }, new[] { e1, e2 },
                    new IAllocateMessageQueueStrategy[] { inner, inner });
                Check("S7e NEARBY 的线上分配 == 内层环的离线预测", converged, detail);
                HashSet<string> k1 = QueueKeys(e1.Assignment()), k2 = QueueKeys(e2.Assignment());
                Check("S7e NEARBY 两实例分配无交集且不漏",
                    !k1.Overlaps(k2)
                    && new HashSet<string>(k1.Union(k2, StringComparer.Ordinal)).SetEquals(QueueKeys(allQueues)),
                    "a=" + k1.Count.ToString(CultureInfo.InvariantCulture)
                    + " b=" + k2.Count.ToString(CultureInfo.InvariantCulture));
                // resolver 真的被 rebalance 调用过，且看到的是真实 brokerName + 两个真实 clientId。
                List<string> brokerCalls = resolver.BrokerCalls();
                var seenBrokers = new HashSet<string>(brokerCalls, StringComparer.Ordinal);
                var realBrokers = new HashSet<string>(allQueues.Select(mq => mq.BrokerName), StringComparer.Ordinal);
                Check("S7e resolver 被逐个队列问过机房（"
                    + brokerCalls.Count.ToString(CultureInfo.InvariantCulture) + " 次）",
                    brokerCalls.Count > 0 && seenBrokers.SetEquals(realBrokers),
                    "brokers=[" + KeySetText(seenBrokers) + "]");
                HashSet<string> calls = new(resolver.ConsumerCalls(), StringComparer.Ordinal);
                Check("S7e resolver 被问过两个真实 clientId",
                    calls.Contains(e1.ClientId) && calls.Contains(e2.ClientId),
                    "calls=[" + KeySetText(calls) + "]");
                e1.Shutdown();
                e2.Shutdown();
            }

            // S7f MACHINE_ROOM：真实 brokerName 是 broker-a，Java 的 Split('@') 只切出 1 段
            // ⇒ 白名单怎么写都筛不出队列。要验的是「配错机房安静饿死」，不是打崩 rebalance。
            {
                var room = new AllocateMessageQueueByMachineRoom(new[] { OneRoom.Room });
                Check("S7f 策略名 = MACHINE_ROOM 且白名单能读回",
                    room.GetName() == "MACHINE_ROOM" && room.GetConsumeridcs().Contains(OneRoom.Room),
                    "idcs=[" + KeySetText(room.GetConsumeridcs()) + "]");
                // 对照组：同组另一个消费者用默认 AVG。两边各自算策略（Java 就是各算各的），
                // 对照组能分到队列 ⇒ 这一组的心跳注册 + 重平衡确实跑起来了，
                // 于是「f1 为空」只能归因于机房筛选，而不是链路没通。
                var avg = new AllocateMessageQueueAveragely();
                var f1 = new DefaultLitePullConsumer(group12);
                var f2 = new DefaultLitePullConsumer(group12);
                SetupConsumer(f1, namesrv, "s7fa", room);
                SetupConsumer(f2, namesrv, "s7fb", avg);
                f1.Start();
                f2.Start();
                List<MessageQueue> ctrl = WaitAssignment(f2);
                Check("S7f 同组对照组（AVG）正常分到队列", ctrl.Count > 0,
                    "ctrl=[" + KeySetText(QueueKeys(ctrl)) + "]");
                Check("S7f 机房不匹配真实 brokerName → 一条都不分（不报错也不误吃）",
                    f1.Assignment().Count == 0, "assignment=[" + KeySetText(QueueKeys(f1.Assignment())) + "]");
                // 对照组按**两个** cid 算 AVG 只拿到自己那半边 —— 它没替配错的那位兜底（Java 同语义）。
                (bool converged, string detail) = WaitUntilPredictionConverged(
                    group12, allQueues, new[] { f1, f2 }, new[] { f1, f2 },
                    new IAllocateMessageQueueStrategy[] { room, avg });
                Check("S7f 两边线上分配各自收敛到自己策略的离线预测", converged, detail);
                List<MessageExt> starved = DrainFor(f1, 5000);
                Check("S7f 被饿死的一方 poll 不到消息也不抛错", starved.Count == 0,
                    "got=" + starved.Count.ToString(CultureInfo.InvariantCulture));
                f1.Shutdown();
                f2.Shutdown();
            }
        }

        c1.Shutdown();

        Console.WriteLine();
        Console.WriteLine("LitePullConsumer: PASS=" + _pass + " FAIL=" + _fail);
        return _fail == 0 ? 0 : 1;
    }
}
