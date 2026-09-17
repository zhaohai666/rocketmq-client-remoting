// CheckForbiddenHook / FilterMessageHook 真机验证（对齐 Java client.hook，
// 与 cpp/examples/live_hook.cpp、python/verify_hook_live.py 同套场景）。
// 用法：rmq hook [namesrv]
//
// 前置：NameServer + Broker 已起（普通配置即可，**不需要** traceTopicEnable）：
//   - CheckForbiddenHook 纯客户端行为（发请求之前拦截），broker 无感；
//   - FilterMessageHook 的拉取路径依赖 broker 按 codeSet 哈希过滤 + 客户端字符串二次过滤；
//   - POP 路径依赖 timerWheelEnable（默认 true）。
//
// 场景 S0–S11 与 Check() 一一对应，全部命中才返回 0。
//
// ⚠ 两条真机踩出来的硬约定（改脚本时勿回退）：
//   1. topic / 消费组一律带 Stamp —— `run_hook_live.sh all` 会让三语言**依次**跑在同一个
//      broker 上，固定名会继承上一轮的提交位点；
//   2. 断言**按 body 前缀过滤** —— 预热消息、以及 broker consumequeue 异步分发都可能让消费者
//      多收几条，数总数会假红/假绿。
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveHook
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

    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 200)
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

    // ---------------- 业务消息收集者（按 body 前缀过滤）----------------
    private sealed class MsgCollector : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<MessageExt> _msgs = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                _msgs.AddRange(msgs);
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        /// <summary>按 body 前缀取消息 —— 预热消息与重复投递都靠它过滤掉。</summary>
        public List<MessageExt> ByPrefix(params string[] prefixes)
        {
            lock (_lk)
            {
                var outList = new List<MessageExt>();
                foreach (MessageExt m in _msgs)
                {
                    string body = BodyText(m);
                    foreach (string p in prefixes)
                    {
                        if (body.StartsWith(p, StringComparison.Ordinal))
                        {
                            outList.Add(m);
                            break;
                        }
                    }
                }

                return outList;
            }
        }
    }

    // ---------------- CheckForbiddenHook 实现 ----------------
    private sealed class ForbidHook : ICheckForbiddenHook
    {
        private readonly bool _forbid;
        private readonly string _onlyTopic;
        private readonly object _lk = new();
        private readonly List<CommunicationMode> _modes = new();

        public ForbidHook(bool forbid, string onlyTopic = "")
        {
            _forbid = forbid;
            _onlyTopic = onlyTopic;
        }

        public int Calls { get; private set; }

        public CheckForbiddenContext? LastContext { get; private set; }

        public List<CommunicationMode> Modes()
        {
            lock (_lk) return new List<CommunicationMode>(_modes);
        }

        public string HookName() => "live-forbid";

        public void CheckForbidden(CheckForbiddenContext context)
        {
            lock (_lk)
            {
                Calls++;
                LastContext = context;
                _modes.Add(context.CommunicationMode);
            }

            if (!_forbid)
            {
                return;
            }

            string topic = context.Mq?.Topic ?? string.Empty;
            if (_onlyTopic.Length == 0 || topic == _onlyTopic
                || topic.StartsWith(_onlyTopic, StringComparison.Ordinal))
            {
                throw new MQClientException("forbidden by live hook");
            }
        }
    }

    // ---------------- FilterMessageHook 实现 ----------------
    /// <summary>摘掉 body 以 drop- 开头的消息（FilterMessageContext.MsgList 是可变列表）。</summary>
    private sealed class DropHook : IFilterMessageHook
    {
        private readonly string _prefix;
        private readonly object _lk = new();
        private readonly List<int> _seen = new();

        public DropHook(string prefix = "drop-")
        {
            _prefix = prefix;
        }

        public int Calls { get; private set; }

        public List<int> Seen()
        {
            lock (_lk) return new List<int>(_seen);
        }

        public string HookName() => "live-drop";

        public void FilterMessage(FilterMessageContext context)
        {
            lock (_lk)
            {
                Calls++;
                _seen.Add(context.MsgList.Count);
            }

            var kept = new List<MessageExt>();
            foreach (MessageExt m in context.MsgList)
            {
                if (!BodyText(m).StartsWith(_prefix, StringComparison.Ordinal))
                {
                    kept.Add(m);
                }
            }

            context.MsgList = kept;
        }
    }

    /// <summary>每次调用都抛异常 —— 用于验证"钩子异常被吞掉、后续钩子照常生效"。</summary>
    private sealed class BoomHook : IFilterMessageHook
    {
        public int Calls { get; private set; }

        public string HookName() => "live-boom";

        public void FilterMessage(FilterMessageContext context)
        {
            Calls++;
            throw new InvalidOperationException("boom from live hook");
        }
    }

    // ---------------- 工具 ----------------
    private static bool Warm(string namesrv, string topic, string producerGroup)
    {
        var p = new DefaultMQProducer(producerGroup + "_warm");
        p.NamesrvAddr = namesrv;
        p.Start();
        try
        {
            p.Send(new Message(topic, Encoding.UTF8.GetBytes("warm-up")), 5000);
            return true;
        }
        catch (Exception e)
        {
            Console.WriteLine("  [warn] warm-up failed for " + topic + ": " + e.Message);
            return false;
        }
        finally
        {
            p.Shutdown();
        }
    }

    private static DefaultMQPushConsumer StartConsumer(string namesrv, string group, string topic,
        MsgCollector collector, bool pop = false, long invisibleMs = 0,
        params IFilterMessageHook[] hooks)
    {
        var c = new DefaultMQPushConsumer(group);
        c.SetNamesrvAddr(namesrv);
        c.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromLastOffset;
        c.Subscribe(topic, "*");
        c.SetMessageListener(collector);
        foreach (IFilterMessageHook h in hooks)
        {
            c.RegisterFilterMessageHook(h);
        }

        if (pop)
        {
            c.PopMode = true;
            if (invisibleMs > 0)
            {
                c.PopInvisibleTime = invisibleMs;
            }

            c.PopBatchNums = 8;
        }

        c.Start();
        return c;
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        string topicCk = "HookCheckForbidden_" + Stamp;
        string topicFilter = "HookFilterPull_" + Stamp;
        string topicTag = "HookFilterTag_" + Stamp;
        string topicBoom = "HookFilterBoom_" + Stamp;
        string topicPop = "HookFilterPop_" + Stamp;
        string group = "GID_hook_live_" + Stamp;
        string producerGroup = "GID_hook_producer_" + Stamp;

        Console.WriteLine("=".PadRight(70, '='));
        Console.WriteLine("RocketMQ hook live verify (.NET): namesrv=" + namesrv + " stamp=" + Stamp);
        Console.WriteLine("=".PadRight(70, '='));

        // ---------- S0 订阅语义自检（本地纯逻辑，防回归）----------
        {
            SubscriptionData subAll = FilterAPI.BuildSubscriptionData("T", "*");
            SubscriptionData subTag = FilterAPI.BuildSubscriptionData("T", "TagA||TagB");
            Check("S0 SUB_ALL 的 tagsSet/codeSet 为空、显式 tag 填 codeSet（Java 语义）",
                subAll.TagsSet.Count == 0 && subAll.CodeSet.Count == 0
                && subTag.TagsSet.Count == 2 && subTag.TagsSet.Contains("TagA")
                && subTag.TagsSet.Contains("TagB")
                && subTag.CodeSet.Contains(2598919) && subTag.CodeSet.Contains(2598920),
                "sub_all=" + subAll.TagsSet.Count.ToString(CultureInfo.InvariantCulture)
                + "/" + subAll.CodeSet.Count.ToString(CultureInfo.InvariantCulture)
                + " tag=" + subTag.TagsSet.Count.ToString(CultureInfo.InvariantCulture)
                + "/" + subTag.CodeSet.Count.ToString(CultureInfo.InvariantCulture));
        }

        // =============== 第一部分：CheckForbiddenHook ===============
        if (!Warm(namesrv, topicCk, producerGroup))
        {
            Check("S1 预热建 topic（CheckForbidden 用）", false);
            return 1;
        }

        var ckCollector = new MsgCollector();
        DefaultMQPushConsumer ckConsumer = StartConsumer(namesrv, group + "_ck", topicCk, ckCollector);
        Check("S1 预热建 topic + 消费者已分配队列",
            WaitUntil(() => ckConsumer.AssignedQueueKeys().Count > 0, 30000),
            "assigned=" + ckConsumer.AssignedQueueKeys().Count.ToString(CultureInfo.InvariantCulture));

        // ---------- S2 放行 ----------
        var allow = new ForbidHook(false);
        var pAllow = new DefaultMQProducer(producerGroup + "_allow");
        pAllow.NamesrvAddr = namesrv;
        pAllow.RetryTimesWhenSendFailed = 2;
        pAllow.RegisterCheckForbiddenHook(allow);
        pAllow.Start();
        bool okAllow = false;
        try
        {
            pAllow.Send(new Message(topicCk, Encoding.UTF8.GetBytes("allowed-1")), 5000);
            okAllow = true;
        }
        catch (Exception e)
        {
            Console.WriteLine("  [warn] allowed send failed: " + e.Message);
        }

        Check("S2 放行钩子：发送成功且钩子被调用 1 次", okAllow && allow.Calls == 1,
            "ok=" + (okAllow ? "true" : "false")
            + " calls=" + allow.Calls.ToString(CultureInfo.InvariantCulture)
            + " modes=" + string.Join(",", allow.Modes()));
        Check("S2b 拦截上下文带上 group / mq / unitMode=false / sendResult=null",
            allow.LastContext is not null
            && allow.LastContext.Group == producerGroup + "_allow"
            && allow.LastContext.Mq is not null && allow.LastContext.Mq.Topic == topicCk
            && !allow.LastContext.UnitMode
            && allow.LastContext.SendResult is null,
            allow.LastContext is null
                ? "no context"
                : "group=" + allow.LastContext.Group
                  + " mq=" + (allow.LastContext.Mq?.Topic ?? "-"));

        // ---------- S3 拦截（每次尝试都跑钩子）----------
        var forbid = new ForbidHook(true, topicCk);
        var pForbid = new DefaultMQProducer(producerGroup + "_forbid");
        pForbid.NamesrvAddr = namesrv;
        pForbid.RetryTimesWhenSendFailed = 2;
        pForbid.RegisterCheckForbiddenHook(forbid);
        pForbid.Start();
        MQClientException? raised = null;
        try
        {
            pForbid.Send(new Message(topicCk, Encoding.UTF8.GetBytes("blocked-1")), 5000);
        }
        catch (MQClientException e)
        {
            raised = e;
        }

        Check("S3 拦截钩子：send 抛 MQClientException，且钩子按 retryTimes+1=3 次调用",
            raised is not null && forbid.Calls == 3,
            "raised=" + (raised is null ? "none" : "MQClientException")
            + " calls=" + forbid.Calls.ToString(CultureInfo.InvariantCulture));

        // ---------- S5 单向发送同样被拦截 ----------
        var forbidOw = new ForbidHook(true, topicCk);
        var pOw = new DefaultMQProducer(producerGroup + "_oneway");
        pOw.NamesrvAddr = namesrv;
        pOw.RegisterCheckForbiddenHook(forbidOw);
        pOw.Start();
        Exception? owRaised = null;
        try
        {
            pOw.SendOneway(new Message(topicCk, Encoding.UTF8.GetBytes("blocked-oneway")));
        }
        catch (Exception e)
        {
            owRaised = e;
        }

        Check("S5 单向发送也被拦截，且上下文 mode=ONEWAY",
            owRaised is not null && forbidOw.Modes().Count == 1
            && forbidOw.Modes()[0] == CommunicationMode.Oneway,
            "raised=" + (owRaised is null ? "none" : owRaised.GetType().Name)
            + " modes=" + string.Join(",", forbidOw.Modes()));

        // ---------- S4 被拦截的消息没有落到 broker ----------
        bool gotCk = WaitUntil(() => ckCollector.ByPrefix("allowed-").Count >= 1, 20000);
        Thread.Sleep(2000);   // 留出"被拦截的消息万一真的发出去了"的到达窗口
        List<MessageExt> landed = ckCollector.ByPrefix("allowed-", "blocked-");
        Check("S4 被拦截的消息没有落到 broker（只有放行的那 1 条）",
            gotCk && landed.Count == 1 && BodyText(landed[0]) == "allowed-1",
            "count=" + landed.Count.ToString(CultureInfo.InvariantCulture)
            + " bodies=" + string.Join(",", landed.ConvertAll(BodyText)));

        pAllow.Shutdown();
        pForbid.Shutdown();
        pOw.Shutdown();
        ckConsumer.Shutdown();

        // =============== 第二部分：FilterMessageHook（拉取路径）===============
        if (!Warm(namesrv, topicFilter, producerGroup))
        {
            Check("S6 预热建 topic（过滤钩子用）", false);
            return 1;
        }

        var drop = new DropHook();
        var filterCollector = new MsgCollector();
        DefaultMQPushConsumer filterConsumer = StartConsumer(namesrv, group + "_filter",
            topicFilter, filterCollector, false, 0, drop);
        if (!WaitUntil(() => filterConsumer.AssignedQueueKeys().Count > 0, 30000))
        {
            Check("S6 消费者已分配到队列", false);
            return 1;
        }

        var p = new DefaultMQProducer(producerGroup + "_f");
        p.NamesrvAddr = namesrv;
        p.Start();
        for (int i = 0; i < 3; i++)
        {
            p.Send(new Message(topicFilter,
                Encoding.UTF8.GetBytes("keep-" + i.ToString(CultureInfo.InvariantCulture))), 5000);
            p.Send(new Message(topicFilter,
                Encoding.UTF8.GetBytes("drop-" + i.ToString(CultureInfo.InvariantCulture))), 5000);
        }

        bool gotF = WaitUntil(() => filterCollector.ByPrefix("keep-").Count >= 3, 25000);
        List<MessageExt> kept = filterCollector.ByPrefix("keep-");
        List<MessageExt> dropped = filterCollector.ByPrefix("drop-");
        Check("S6 过滤钩子在拉取路径生效：3 收 2 丢",
            gotF && kept.Count == 3 && dropped.Count == 0,
            "keep=" + kept.Count.ToString(CultureInfo.InvariantCulture)
            + " drop=" + dropped.Count.ToString(CultureInfo.InvariantCulture)
            + " hook_calls=" + drop.Calls.ToString(CultureInfo.InvariantCulture)
            + " seen=" + string.Join("/", drop.Seen()));

        // ---------- S7 被摘掉的消息不重投 ----------
        Thread.Sleep(8000);
        Check("S7 被摘掉的消息不会重投（位点已推进，等 8s 计数不变）",
            filterCollector.ByPrefix("drop-").Count == 0
            && filterCollector.ByPrefix("keep-").Count == 3,
            "keep=" + filterCollector.ByPrefix("keep-").Count.ToString(CultureInfo.InvariantCulture)
            + " drop=" + filterCollector.ByPrefix("drop-").Count.ToString(CultureInfo.InvariantCulture));
        p.Shutdown();
        filterConsumer.Shutdown();

        // =============== 第三部分：客户端二次 tag 过滤 ===============
        if (!Warm(namesrv, topicTag, producerGroup))
        {
            Check("S8 预热建 topic（tag 过滤用）", false);
            return 1;
        }

        var tagCollector = new MsgCollector();
        var tagConsumer = new DefaultMQPushConsumer(group + "_tag");
        tagConsumer.SetNamesrvAddr(namesrv);
        tagConsumer.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromLastOffset;
        tagConsumer.Subscribe(topicTag, "TagA");      // tagsSet={TagA} → 客户端会二次过滤
        tagConsumer.SetMessageListener(tagCollector);
        tagConsumer.Start();
        if (!WaitUntil(() => tagConsumer.AssignedQueueKeys().Count > 0, 30000))
        {
            Check("S8 消费者已分配到队列（tag）", false);
            return 1;
        }

        var p2 = new DefaultMQProducer(producerGroup + "_tag");
        p2.NamesrvAddr = namesrv;
        p2.Start();
        for (int i = 0; i < 2; i++)
        {
            p2.Send(new Message(topicTag,
                Encoding.UTF8.GetBytes("tagA-" + i.ToString(CultureInfo.InvariantCulture)))
            { Tags = "TagA" }, 5000);
            p2.Send(new Message(topicTag,
                Encoding.UTF8.GetBytes("tagB-" + i.ToString(CultureInfo.InvariantCulture)))
            { Tags = "TagB" }, 5000);
        }

        bool gotT = WaitUntil(() => tagCollector.ByPrefix("tagA-").Count >= 2, 25000);
        Thread.Sleep(2000);
        Check("S8 订阅 TagA：只收到 TagA 的 2 条（broker 哈希过滤 + 客户端二次过滤）",
            gotT && tagCollector.ByPrefix("tagA-").Count == 2
            && tagCollector.ByPrefix("tagB-").Count == 0,
            "tagA=" + tagCollector.ByPrefix("tagA-").Count.ToString(CultureInfo.InvariantCulture)
            + " tagB=" + tagCollector.ByPrefix("tagB-").Count.ToString(CultureInfo.InvariantCulture));
        p2.Shutdown();
        tagConsumer.Shutdown();

        // =============== 第四部分：钩子异常不影响消费 ===============
        if (!Warm(namesrv, topicBoom, producerGroup))
        {
            Check("S9 预热建 topic（异常钩子用）", false);
            return 1;
        }

        var boom = new BoomHook();
        var drop2 = new DropHook();
        var boomCollector = new MsgCollector();
        DefaultMQPushConsumer boomConsumer = StartConsumer(namesrv, group + "_boom",
            topicBoom, boomCollector, false, 0, boom, drop2);
        if (!WaitUntil(() => boomConsumer.AssignedQueueKeys().Count > 0, 30000))
        {
            Check("S9 消费者已分配到队列（boom）", false);
            return 1;
        }

        var p3 = new DefaultMQProducer(producerGroup + "_boom");
        p3.NamesrvAddr = namesrv;
        p3.Start();
        p3.Send(new Message(topicBoom, Encoding.UTF8.GetBytes("keep-boom")), 5000);
        p3.Send(new Message(topicBoom, Encoding.UTF8.GetBytes("drop-boom")), 5000);
        bool gotB = WaitUntil(() => boomCollector.ByPrefix("keep-").Count >= 1, 25000);
        Thread.Sleep(2000);
        Check("S9 前一个钩子抛异常被吞掉、后续钩子照常生效（异常不影响消费）",
            gotB && boom.Calls >= 1 && drop2.Calls >= 1
            && boomCollector.ByPrefix("keep-").Count == 1
            && boomCollector.ByPrefix("drop-").Count == 0,
            "boom_calls=" + boom.Calls.ToString(CultureInfo.InvariantCulture)
            + " drop_calls=" + drop2.Calls.ToString(CultureInfo.InvariantCulture)
            + " keep=" + boomCollector.ByPrefix("keep-").Count.ToString(CultureInfo.InvariantCulture)
            + " drop=" + boomCollector.ByPrefix("drop-").Count.ToString(CultureInfo.InvariantCulture));
        p3.Shutdown();
        boomConsumer.Shutdown();

        // =============== 第五部分：FilterMessageHook（POP 路径，摘掉即 ack）===============
        if (!Warm(namesrv, topicPop, producerGroup))
        {
            Check("S10 预热建 topic（POP 用）", false);
            return 1;
        }

        var popDrop = new DropHook();
        var popCollector = new MsgCollector();
        const long invisibleMs = 10000;
        DefaultMQPushConsumer popConsumer = StartConsumer(namesrv, group + "_pop",
            topicPop, popCollector, true, invisibleMs, popDrop);
        if (!WaitUntil(() => popConsumer.AssignedQueueKeys().Count > 0, 30000))
        {
            Check("S10 POP 消费者已分配到队列", false);
            return 1;
        }

        var p4 = new DefaultMQProducer(producerGroup + "_pop");
        p4.NamesrvAddr = namesrv;
        p4.Start();
        for (int i = 0; i < 2; i++)
        {
            p4.Send(new Message(topicPop,
                Encoding.UTF8.GetBytes("keep-pop-" + i.ToString(CultureInfo.InvariantCulture))), 5000);
        }

        p4.Send(new Message(topicPop, Encoding.UTF8.GetBytes("drop-pop-0")), 5000);

        bool gotP = WaitUntil(() => popCollector.ByPrefix("keep-").Count >= 2, 30000);
        Check("S10 POP 路径过滤钩子生效：2 收 1 丢",
            gotP && popCollector.ByPrefix("keep-").Count == 2
            && popCollector.ByPrefix("drop-").Count == 0,
            "keep=" + popCollector.ByPrefix("keep-").Count.ToString(CultureInfo.InvariantCulture)
            + " drop=" + popCollector.ByPrefix("drop-").Count.ToString(CultureInfo.InvariantCulture)
            + " hook_calls=" + popDrop.Calls.ToString(CultureInfo.InvariantCulture));

        // ---------- S11 被摘掉的那条已 ack：观察窗口必须 > invisibleTime，否则假绿 ----------
        Console.WriteLine("  ... 等待 " + ((invisibleMs / 1000.0) + 6).ToString("0",
            CultureInfo.InvariantCulture) + "s（> invisibleTime="
            + (invisibleMs / 1000.0).ToString("0", CultureInfo.InvariantCulture)
            + "s）确认被摘掉的消息不复活");
        Thread.Sleep((int)invisibleMs + 6000);
        Check("S11 POP 路径被摘掉的消息已 ack（观测窗 > invisibleTime，未复活重投）",
            popCollector.ByPrefix("keep-").Count == 2
            && popCollector.ByPrefix("drop-").Count == 0,
            "keep=" + popCollector.ByPrefix("keep-").Count.ToString(CultureInfo.InvariantCulture)
            + " drop=" + popCollector.ByPrefix("drop-").Count.ToString(CultureInfo.InvariantCulture));
        p4.Shutdown();
        popConsumer.Shutdown();

        Console.WriteLine("########################################");
        Console.WriteLine("PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
            + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture));
        return _fail == 0 ? 0 : 1;
    }
}
