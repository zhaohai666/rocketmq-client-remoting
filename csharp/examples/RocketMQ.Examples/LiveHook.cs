// Prerequisite: a NameServer + Broker are up (an ordinary configuration is enough,
// **traceTopicEnable is not needed**):
//   - CheckForbiddenHook is pure client behaviour (it intercepts before sending the
//     request), the broker is unaware of it;
//   - FilterMessageHook's pull path depends on the broker filtering by codeSet hash plus a
//     second client-side string filter;
//   - the POP path depends on timerWheelEnable (true by default).
//
// Scenarios S0-S11 map one to one onto Check(); it returns 0 only when everything hits.
//
// Two hard conventions learned on a real machine (do not regress them when changing the
  // script):
//   1. topics / consumer groups always carry a Stamp -- several rounds of real-machine
    // integration run **one after another** against the same broker and a fixed name would
  // inherit the previous round's committed offsets;
//   2. assertions **filter by body prefix** -- the warm-up message and the broker's
    // asynchronous consumequeue dispatch can both make the consumer receive a few extra,
    // so counting the total gives false reds / false greens.
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

    // ---------------- The business message collector (filters by body prefix)----------------
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

  /// <summary>Takes messages by body prefix -- it is what filters out the warm-up messages
    /// and duplicate deliveries.</summary>
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

    // ---------------- The CheckForbiddenHook implementation ----------------
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

    // ---------------- The FilterMessageHook implementation ----------------
    /// <summary>Picks out the messages whose body starts with drop- (FilterMessageContext.MsgList is a mutable list).</summary>
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

  /// <summary>Throws on every call -- used to verify that "a hook exception is swallowed and
    /// the later hooks still take effect".</summary>
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

    // ---------------- Utilities ----------------
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
        Console.WriteLine("RocketMQ hook live verify (C#): namesrv=" + namesrv + " stamp=" + Stamp);
        Console.WriteLine("=".PadRight(70, '='));

        // ---------- S0 subscription semantics self-check (pure local logic, regression guard)----------
        {
            SubscriptionData subAll = FilterAPI.BuildSubscriptionData("T", "*");
            SubscriptionData subTag = FilterAPI.BuildSubscriptionData("T", "TagA||TagB");
            Check("S0 SUB_ALL has an empty tagsSet/codeSet while an explicit tag fills codeSet (the protocol semantics)",
                subAll.TagsSet.Count == 0 && subAll.CodeSet.Count == 0
                && subTag.TagsSet.Count == 2 && subTag.TagsSet.Contains("TagA")
                && subTag.TagsSet.Contains("TagB")
                && subTag.CodeSet.Contains(2598919) && subTag.CodeSet.Contains(2598920),
                "sub_all=" + subAll.TagsSet.Count.ToString(CultureInfo.InvariantCulture)
                + "/" + subAll.CodeSet.Count.ToString(CultureInfo.InvariantCulture)
                + " tag=" + subTag.TagsSet.Count.ToString(CultureInfo.InvariantCulture)
                + "/" + subTag.CodeSet.Count.ToString(CultureInfo.InvariantCulture));
        }

        // =============== Part one: CheckForbiddenHook ===============
        if (!Warm(namesrv, topicCk, producerGroup))
        {
            Check("S1 warm-up created the topic (for CheckForbidden)", false);
            return 1;
        }

        var ckCollector = new MsgCollector();
        DefaultMQPushConsumer ckConsumer = StartConsumer(namesrv, group + "_ck", topicCk, ckCollector);
         Check("S1 the warm-up created the topic and the consumer is assigned queues",
            WaitUntil(() => ckConsumer.AssignedQueueKeys().Count > 0, 30000),
            "assigned=" + ckConsumer.AssignedQueueKeys().Count.ToString(CultureInfo.InvariantCulture));

        // ---------- S2 let through ----------
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

         Check("S2 the allowing hook: the send succeeded and the hook was called once", okAllow && allow.Calls == 1,
            "ok=" + (okAllow ? "true" : "false")
            + " calls=" + allow.Calls.ToString(CultureInfo.InvariantCulture)
            + " modes=" + string.Join(",", allow.Modes()));
   Check("S2b the interception context carries group / mq / unitMode=false / sendResult=null",
            allow.LastContext is not null
            && allow.LastContext.Group == producerGroup + "_allow"
            && allow.LastContext.Mq is not null && allow.LastContext.Mq.Topic == topicCk
            && !allow.LastContext.UnitMode
            && allow.LastContext.SendResult is null,
            allow.LastContext is null
                ? "no context"
                : "group=" + allow.LastContext.Group
                  + " mq=" + (allow.LastContext.Mq?.Topic ?? "-"));

        // ---------- S3 interception (the hook runs on every attempt)----------
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

        Check("S3 the intercepting hook: send throws MQClientException and the hook was called retryTimes+1=3 times",
            raised is not null && forbid.Calls == 3,
            "raised=" + (raised is null ? "none" : "MQClientException")
            + " calls=" + forbid.Calls.ToString(CultureInfo.InvariantCulture));

        // ---------- S5 a oneway send is intercepted too ----------
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

   Check("S5 the oneway send is intercepted too, and the context mode is ONEWAY",
            owRaised is not null && forbidOw.Modes().Count == 1
            && forbidOw.Modes()[0] == CommunicationMode.Oneway,
            "raised=" + (owRaised is null ? "none" : owRaised.GetType().Name)
            + " modes=" + string.Join(",", forbidOw.Modes()));

        // ---------- S4 the intercepted messages never reached the broker ----------
        bool gotCk = WaitUntil(() => ckCollector.ByPrefix("allowed-").Count >= 1, 20000);
   Thread.Sleep(2000);   // leave room for the arrival window "in case an intercepted message really went out"
        List<MessageExt> landed = ckCollector.ByPrefix("allowed-", "blocked-");
        Check("S4 the intercepted message did not land on the broker (only the 1 that was let through)",
            gotCk && landed.Count == 1 && BodyText(landed[0]) == "allowed-1",
            "count=" + landed.Count.ToString(CultureInfo.InvariantCulture)
            + " bodies=" + string.Join(",", landed.ConvertAll(BodyText)));

        pAllow.Shutdown();
        pForbid.Shutdown();
        pOw.Shutdown();
        ckConsumer.Shutdown();

    // =============== Part two: FilterMessageHook (the pull path)===============
        if (!Warm(namesrv, topicFilter, producerGroup))
        {
            Check("S6 warm-up created the topic (for the filter hook)", false);
            return 1;
        }

        var drop = new DropHook();
        var filterCollector = new MsgCollector();
        DefaultMQPushConsumer filterConsumer = StartConsumer(namesrv, group + "_filter",
            topicFilter, filterCollector, false, 0, drop);
        if (!WaitUntil(() => filterConsumer.AssignedQueueKeys().Count > 0, 30000))
        {
          Check("S6 the consumer was assigned queues", false);
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
   Check("S6 the filter hook takes effect on the pull path: 3 received 2 dropped",
            gotF && kept.Count == 3 && dropped.Count == 0,
            "keep=" + kept.Count.ToString(CultureInfo.InvariantCulture)
            + " drop=" + dropped.Count.ToString(CultureInfo.InvariantCulture)
            + " hook_calls=" + drop.Calls.ToString(CultureInfo.InvariantCulture)
            + " seen=" + string.Join("/", drop.Seen()));

        // ---------- S7 the dropped messages are not resent ----------
        Thread.Sleep(8000);
      Check("S7 the dropped messages are not resent (the offset already advanced, the count is unchanged after 8s)",
            filterCollector.ByPrefix("drop-").Count == 0
            && filterCollector.ByPrefix("keep-").Count == 3,
            "keep=" + filterCollector.ByPrefix("keep-").Count.ToString(CultureInfo.InvariantCulture)
            + " drop=" + filterCollector.ByPrefix("drop-").Count.ToString(CultureInfo.InvariantCulture));
        p.Shutdown();
        filterConsumer.Shutdown();

    // =============== Part three: the client-side second tag filter ===============
        if (!Warm(namesrv, topicTag, producerGroup))
        {
            Check("S8 warm-up created the topic (for tag filtering)", false);
            return 1;
        }

        var tagCollector = new MsgCollector();
        var tagConsumer = new DefaultMQPushConsumer(group + "_tag");
        tagConsumer.SetNamesrvAddr(namesrv);
        tagConsumer.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromLastOffset;
        tagConsumer.Subscribe(topicTag, "TagA");      // tagsSet={TagA} -> the client filters a second time
        tagConsumer.SetMessageListener(tagCollector);
        tagConsumer.Start();
        if (!WaitUntil(() => tagConsumer.AssignedQueueKeys().Count > 0, 30000))
        {
    Check("S8 the consumer was assigned queues (tag)", false);
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
 Check("S8 subscribing to TagA: only the 2 TagA messages are received (broker hash filter + client-side second filter)",
            gotT && tagCollector.ByPrefix("tagA-").Count == 2
            && tagCollector.ByPrefix("tagB-").Count == 0,
            "tagA=" + tagCollector.ByPrefix("tagA-").Count.ToString(CultureInfo.InvariantCulture)
            + " tagB=" + tagCollector.ByPrefix("tagB-").Count.ToString(CultureInfo.InvariantCulture));
        p2.Shutdown();
        tagConsumer.Shutdown();

    // =============== Part four: a hook exception does not affect consumption ===============
        if (!Warm(namesrv, topicBoom, producerGroup))
        {
    Check("S9 warm-up created the topic (for the exception hook)", false);
            return 1;
        }

        var boom = new BoomHook();
        var drop2 = new DropHook();
        var boomCollector = new MsgCollector();
        DefaultMQPushConsumer boomConsumer = StartConsumer(namesrv, group + "_boom",
            topicBoom, boomCollector, false, 0, boom, drop2);
        if (!WaitUntil(() => boomConsumer.AssignedQueueKeys().Count > 0, 30000))
        {
      Check("S9 the consumer was assigned queues (boom)", false);
            return 1;
        }

        var p3 = new DefaultMQProducer(producerGroup + "_boom");
        p3.NamesrvAddr = namesrv;
        p3.Start();
        p3.Send(new Message(topicBoom, Encoding.UTF8.GetBytes("keep-boom")), 5000);
        p3.Send(new Message(topicBoom, Encoding.UTF8.GetBytes("drop-boom")), 5000);
        bool gotB = WaitUntil(() => boomCollector.ByPrefix("keep-").Count >= 1, 25000);
        Thread.Sleep(2000);
        Check("S9 the previous hook's exception was swallowed and the later hook still took effect (the exception does not affect consumption)",
            gotB && boom.Calls >= 1 && drop2.Calls >= 1
            && boomCollector.ByPrefix("keep-").Count == 1
            && boomCollector.ByPrefix("drop-").Count == 0,
            "boom_calls=" + boom.Calls.ToString(CultureInfo.InvariantCulture)
            + " drop_calls=" + drop2.Calls.ToString(CultureInfo.InvariantCulture)
            + " keep=" + boomCollector.ByPrefix("keep-").Count.ToString(CultureInfo.InvariantCulture)
            + " drop=" + boomCollector.ByPrefix("drop-").Count.ToString(CultureInfo.InvariantCulture));
        p3.Shutdown();
        boomConsumer.Shutdown();

    // =============== Part five: FilterMessageHook (the POP path, dropping it means acking)===============
        if (!Warm(namesrv, topicPop, producerGroup))
        {
            Check("S10 warm-up created the topic (for POP)", false);
            return 1;
        }

        var popDrop = new DropHook();
        var popCollector = new MsgCollector();
        const long invisibleMs = 10000;
        DefaultMQPushConsumer popConsumer = StartConsumer(namesrv, group + "_pop",
            topicPop, popCollector, true, invisibleMs, popDrop);
        if (!WaitUntil(() => popConsumer.AssignedQueueKeys().Count > 0, 30000))
        {
            Check("S10 the POP consumer was assigned queues", false);
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
        Check("S10 the filter hook takes effect on the POP path: 2 received 1 dropped",
            gotP && popCollector.ByPrefix("keep-").Count == 2
            && popCollector.ByPrefix("drop-").Count == 0,
            "keep=" + popCollector.ByPrefix("keep-").Count.ToString(CultureInfo.InvariantCulture)
            + " drop=" + popCollector.ByPrefix("drop-").Count.ToString(CultureInfo.InvariantCulture)
            + " hook_calls=" + popDrop.Calls.ToString(CultureInfo.InvariantCulture));

        // ---------- S11 the dropped one was acked: the observation window must be > invisibleTime, otherwise it is a false green ----------
        Console.WriteLine("  ... waiting " + ((invisibleMs / 1000.0) + 6).ToString("0",
            CultureInfo.InvariantCulture) + "s (> invisibleTime="
            + (invisibleMs / 1000.0).ToString("0", CultureInfo.InvariantCulture)
            + "s) to confirm the dropped message does not come back");
        Thread.Sleep((int)invisibleMs + 6000);
        Check("S11 the message dropped on the POP path was acked (observation window > invisibleTime, not revived and redelivered)",
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
