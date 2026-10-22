// Real-machine proof of the SET_MESSAGE_REQUEST_MODE(401) request shape on a live broker, driven
// through the production admin method and read back with GET_ALL_MESSAGE_REQUEST_MODE(402).
//
// Usage: rmq pop-mode <namesrv> [legs] [stamp]
//        legs: all (default) or a comma list of l1,l2,l3
//        stamp: unix seconds for the resource names (a fresh one is generated when omitted)
//
//   dotnet run --no-build --project examples/RocketMQ.Examples -- pop-mode 127.0.0.1:9876
//
// Wire contract under test: on the wire a 401 has NO custom header (no ext fields) at all. The
// broker decodes the request body as ONE JSON object {"topic":.., "consumerGroup":.., "mode":..,
// "popShareQueueNum":..} and reads all four values out of that object alone: `mode` is the string
// "POP" or "PULL", `popShareQueueNum` is a JSON number that has to be present even when it is 0.
// The production admin write builds exactly that body and sends it with an empty header.
//
// Measured fact on a live broker, confirmed repeatedly: a 401 whose four values instead ride the
// header while the body stays empty leaves the broker with nothing to decode. It answers a system
// error whose remark reports the decoded request body as null, and the mode does NOT switch. The
// header form is REJECTED, not silently accepted.
//
// The only in-protocol way to see whether a mode switched is 402, which likewise carries no body
// and no header; its RESPONSE body is {"messageRequestModeMap":
// {"<topic>": {"<group>": {topic, consumerGroup, mode, popShareQueueNum}}}}. A broker that never
// had a mode set answers an error with an empty body, so "no usable body" reads here as an empty
// map and an absent entry becomes the single signal to assert.
//
// Three legs on ONE freshly created topic (the topic has to exist before any 401: a broker refuses
// a 401 for a topic it has no config for, and that refusal would otherwise look like a broken
// request shape; the subscription group is left to the broker's autoCreate):
//   L1 POSITIVE. The production POP write (popShareQueueNum 4) must land: 402 has an entry keyed by
//      exactly this topic and this group, mode POP, popShareQueueNum the JSON number 4.
//   L2 REPLACEMENT. A second production write of the same topic+group (PULL, 0) is what 402 then
//      reports, so the read-back shows the latest write and not what L1 left behind.
//   L3 PAIRED CONTROL. A RAW 401 for a second, different group on the same topic, its four values
//      in the header and no body: the code and remark are printed, the code is required not SUCCESS
//      and the control group required absent under this topic. Absence only means something against
//      a served read-back, so the leg first anchors the map with one body-form write that is visible
//      in the very same read-back.
//
// Resource names carry a unix-second stamp, so an entry left by an older run can never satisfy an
// assertion, and every readiness wait is polled against broker state under an explicit budget -- no
// live assertion depends on a bare sleep. Cleanup deletes the topic best effort: a failed delete
// prints a note and never turns the run red.
//
// Final line: "############ PASS=<n> FAIL=<n> ############"; exit 0 with FAIL=0, 2 for bad
// arguments (nothing is started, so no connection is made), 1 for a failed assertion.
using System.Globalization;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LivePopMode
{
    /// <summary>Every raw RPC (route, 401 control, 402 read-back) and every admin call carries this
    /// explicit budget instead of the transport's own default.</summary>
    private const int RpcTimeoutMs = 10000;

    /// <summary>Budgets: the broker accepts the create and the name server learns the route, then a
    /// 402 read-back shows the state the last write declared.</summary>
    private const int CreateBudgetMs = 30000;
    private const int RouteBudgetMs = 30000;
    private const int ReadBudgetMs = 15000;
    private const int PollIntervalMs = 250;

    private const string ModePop = "POP";
    private const string ModePull = "PULL";

    /// <summary>L1, L2 and the L3 anchor each get their own number, so a value seen in 402 names the
    /// write that produced it.</summary>
    private const int PopShareNum = 4;
    private const int PullShareNum = 0;
    private const int AnchorShareNum = 1;

    private static string _stamp = string.Empty;
    private static int _pass;
    private static int _fail;

    // ---------------------------------------------------------------- reporting

    private static string N(long v) => v.ToString(CultureInfo.InvariantCulture);

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

    private static string Clip(string s) => s.Length > 200 ? s[..200] + "..." : s;

    private static string Join(IEnumerable<string> v) => "[" + string.Join(", ", v) + "]";

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    /// <summary>Polls a condition until it holds or the budget runs out; the caller prints the scene
    /// either way, so a timeout is evidence rather than a hang.</summary>
    private static bool WaitUntil(Func<bool> ready, int budgetMs, int stepMs)
    {
        long deadline = NowMs() + budgetMs;
        while (!ready())
        {
            if (NowMs() >= deadline)
            {
                return false;
            }

            Thread.Sleep(stepMs);
        }

        return true;
    }

    // ---------------------------------------------------------------- the 402 read-back

    /// <summary>One 402 answer: the broker code and remark plus the decoded messageRequestModeMap. A
    /// transport error, a non-SUCCESS code, an empty body or a body without a map all hand back an
    /// empty map, because the assertion these feed is "this entry is absent".</summary>
    private sealed record Readback(int Code, string Remark, JsonValue Map, string TransportError)
    {
        public bool Answered() => TransportError.Length == 0;

        /// <summary>The printed scene. The map is shown for the topic under test only: the rest of
        /// the broker's table is not evidence and would bury the entry.</summary>
        public string Scene(string topic) => "readback code=" + N(Code) + " remark='" + Clip(Remark)
            + "' topicView=" + Clip(Map.Find(topic)?.Dump() ?? "{}")
            + (TransportError.Length > 0 ? " transport=" + Clip(TransportError) : "");
    }

    /// <summary>Sends a raw 402 -- no body, no custom header -- to the broker that owns the topic.</summary>
    private static Readback ReadModeMap(MQClientInstance client, string addr)
    {
        try
        {
            RemotingCommand response = client.InvokeSyncRaw(addr, RequestCode.GetAllMessageRequestMode,
                new PropertyMap(), null, false, RpcTimeoutMs);
            JsonValue map = JsonValue.MakeObject();
            if (response.Code == ResponseCode.Success && response.Body.Length > 0
                && RemotingSerializable.Decode(response.Body, out JsonValue wrapper)
                && wrapper.Find("messageRequestModeMap") is { IsObject: true } found)
            {
                map = found;
            }

            return new Readback(response.Code, response.Remark, map, string.Empty);
        }
        catch (Exception e)
        {
            return new Readback(-1, string.Empty, JsonValue.MakeObject(),
                e.GetType().Name + ": " + Clip(e.Message));
        }
    }

    /// <summary>The one map entry keyed by exactly this topic and this group.</summary>
    private static JsonValue? EntryOf(JsonValue map, string topic, string group) =>
        map.Find(topic)?.Find(group);

    private static string Describe(JsonValue? entry) =>
        entry is null ? "absent" : Clip(entry.Dump());

    /// <summary>mode as the STRING and popShareQueueNum as a JSON NUMBER: a stringified number would
    /// not be the value the request declared.</summary>
    private static bool ModeIs(JsonValue? entry, string mode, long shareNum)
    {
        if (entry is null || !entry.TryGetString("mode", out string got) || got != mode)
        {
            return false;
        }

        JsonValue? num = entry.Find("popShareQueueNum");
        return num is not null && num.IsNumber && num.IntValue() == shareNum;
    }

    /// <summary>Reads 402 until this topic+group entry satisfies the wanted state or the budget runs
    /// out, returning the last answer so a failed assertion prints what the broker really gave.</summary>
    private static Readback PollEntry(MQClientInstance client, string addr, string topic,
        string group, Func<JsonValue?, bool> want, out JsonValue? entry)
    {
        Readback last = ReadModeMap(client, addr);
        JsonValue? found = EntryOf(last.Map, topic, group);
        WaitUntil(() =>
        {
            last = ReadModeMap(client, addr);
            found = EntryOf(last.Map, topic, group);
            return want(found);
        }, ReadBudgetMs, PollIntervalMs);
        entry = found;
        return last;
    }

    // ---------------------------------------------------------------- context

    /// <summary>The production admin (the 401 writes under test and the cleanup) and the client
    /// instance under it (the raw 401 control and the 402 read-back), aimed at the MASTER broker the
    /// topic's own route names -- never a hardcoded port.</summary>
    private sealed record Ctx(DefaultMQAdminExt Admin, MQClientInstance Client, string BrokerName,
        string BrokerAddr, string Topic, string Group, string CtrlGroup)
    {
        public Readback Read() => ReadModeMap(Client, BrokerAddr);

        public Readback Until(string group, Func<JsonValue?, bool> want, out JsonValue? entry) =>
            PollEntry(Client, BrokerAddr, Topic, group, want, out entry);
    }

    /// <summary>One production 401 write. The admin call throws when the broker answers a non-SUCCESS
    /// code, so an empty return string means the broker accepted the request shape.</summary>
    private static string WriteViaProduction(Ctx ctx, string group, string mode, int shareNum)
    {
        try
        {
            ctx.Admin.SetMessageRequestMode(ctx.BrokerAddr, ctx.Topic, group, mode, shareNum);
            return string.Empty;
        }
        catch (MQBrokerException e)
        {
            return "broker answered code=" + N(e.ResponseCode) + " '" + Clip(e.Message) + "'";
        }
        catch (Exception e)
        {
            return e.GetType().Name + ": " + Clip(e.Message);
        }
    }

    // ---------------------------------------------------------------- L1

    private static void LegPositive(Ctx ctx)
    {
        Console.WriteLine("=== L1 the production 401 switches the mode (" + ctx.Topic + " / "
            + ctx.Group + ") ===");
        string err = WriteViaProduction(ctx, ctx.Group, ModePop, PopShareNum);
        Check("L1 the production POP write (body form, popShareQueueNum 4) was accepted by the "
            + "broker on 401", err.Length == 0, err);
        if (err.Length > 0)
        {
            return;
        }

        Readback read = ctx.Until(ctx.Group, e => ModeIs(e, ModePop, PopShareNum), out JsonValue? entry);
        Check("L1 402 answers an entry keyed by exactly this topic and this group, so the body-form "
            + "write reached the mode table", entry is not null, read.Scene(ctx.Topic));
        Check("L1 that entry's mode is the string POP and its popShareQueueNum is the JSON number 4, "
            + "proving the body-form request really switched the request mode on the broker",
            ModeIs(entry, ModePop, PopShareNum),
            "entry=" + Describe(entry) + " " + read.Scene(ctx.Topic));
    }

    // ---------------------------------------------------------------- L2

    private static void LegReplacement(Ctx ctx)
    {
        Console.WriteLine("=== L2 a second production write replaces the first (" + ctx.Topic
            + " / " + ctx.Group + ") ===");
        string err = WriteViaProduction(ctx, ctx.Group, ModePull, PullShareNum);
        Check("L2 the production PULL write (body form, popShareQueueNum 0) was accepted by the "
            + "broker on 401", err.Length == 0, err);
        if (err.Length > 0)
        {
            return;
        }

        Readback read = ctx.Until(ctx.Group, e => ModeIs(e, ModePull, PullShareNum), out JsonValue? entry);
        Check("L2 the same topic+group key is still in 402 after the replacement, so the second write "
            + "updated the entry instead of adding a second one", entry is not null,
            read.Scene(ctx.Topic));
        Check("L2 that entry now reads mode PULL and popShareQueueNum the JSON number 0, so the "
            + "read-back reports the second write and not what the first one left",
            ModeIs(entry, ModePull, PullShareNum),
            "entry=" + Describe(entry) + " " + read.Scene(ctx.Topic));
    }

    // ---------------------------------------------------------------- L3

    private static void LegHeaderControl(Ctx ctx)
    {
        Console.WriteLine("=== L3 paired control: a 401 whose values ride the header, body empty ("
            + ctx.Topic + " / " + ctx.CtrlGroup + ") ===");

        // The anchor: a body-form write whose value nothing else in this run produces. Without it,
        // "the control group is absent" could be explained by an empty or a stale map.
        string anchorErr = WriteViaProduction(ctx, ctx.Group, ModePop, AnchorShareNum);
        Check("L3 the body-form anchor write for the main group was accepted on 401",
            anchorErr.Length == 0, anchorErr);
        if (anchorErr.Length > 0)
        {
            return;
        }

        Readback anchored = ctx.Until(ctx.Group, e => ModeIs(e, ModePop, AnchorShareNum),
            out JsonValue? seen);
        Check("L3 the anchor write is visible in 402 before the control is sent, so the read-back is "
            + "live for this topic and a later absence is a statement about the control",
            ModeIs(seen, ModePop, AnchorShareNum), "anchor=" + Describe(seen)
                + " " + anchored.Scene(ctx.Topic));

        // The control frame: the same four values, but as ext fields, with nothing to decode.
        var header = new PropertyMap
        {
            ["topic"] = ctx.Topic,
            ["consumerGroup"] = ctx.CtrlGroup,
            ["mode"] = ModePop,
            ["popShareQueueNum"] = N(AnchorShareNum),
        };
        var probe = RemotingCommand.CreateRequestCommand(RequestCode.SetMessageRequestMode, null);
        foreach (KeyValuePair<string, string> kv in header)
        {
            probe.ExtFields[kv.Key] = kv.Value;
        }

        RemotingCommand onWire = RemotingCommand.Decode(probe.Encode());
        Check("L3 the control request really is the header form: four custom header fields, code 401 "
            + "and no request body for the broker to decode",
            onWire.Code == RequestCode.SetMessageRequestMode && onWire.ExtFields.Count == 4
            && onWire.Body.Length == 0,
            "code=" + N(onWire.Code) + " extFields=" + N(onWire.ExtFields.Count)
                + " bodyBytes=" + N(onWire.Body.Length) + " header=" + Join(header.Keys));

        int code;
        string remark;
        try
        {
            RemotingCommand response = ctx.Client.InvokeSyncRaw(ctx.BrokerAddr,
                RequestCode.SetMessageRequestMode, header, null, false, RpcTimeoutMs);
            code = response.Code;
            remark = response.Remark;
        }
        catch (Exception ex)
        {
            Check("L3 the header-form control got an answer from the broker", false,
                ex.GetType().Name + ": " + Clip(ex.Message));
            return;
        }

        Console.WriteLine("  [L3] header-form 401 answered code=" + N(code) + " remark='"
            + Clip(remark) + "'");
        Check("L3 the broker rejected the header-form control (its code is not SUCCESS), so the "
            + "switched modes of L1 and L2 cannot be credited to the header",
            code != ResponseCode.Success, "code=" + N(code) + " remark='" + Clip(remark) + "'");

        Readback read = ctx.Read();
        JsonValue? anchor = EntryOf(read.Map, ctx.Topic, ctx.Group);
        JsonValue? control = EntryOf(read.Map, ctx.Topic, ctx.CtrlGroup);
        Check("L3 the same single read-back answered and still reports the anchor group under this "
            + "topic as POP with the number 1, so the map the control is judged against is neither "
            + "empty nor stale nor an unreachable broker",
            read.Answered() && ModeIs(anchor, ModePop, AnchorShareNum),
            "anchor=" + Describe(anchor) + " " + read.Scene(ctx.Topic));
        Check("L3 the header-form control left no entry for its group under this topic: the mode did "
            + "NOT switch", control is null, "control entry=" + Describe(control)
                + " " + read.Scene(ctx.Topic));
    }

    // ---------------------------------------------------------------- setup

    /// <summary>Creates the topic on the broker and waits, polled and bounded, until the name server
    /// serves its route; the MASTER address that route names for the topic's queue broker is the one
    /// 401 and 402 go to. Returns null and says why when the topic is not usable.</summary>
    private static Ctx? Prepare(DefaultMQAdminExt admin, MQClientInstance client, string topic,
        string group, string ctrlGroup, out string problem)
    {
        problem = string.Empty;
        // The broker answers the create before the name server knows the topic, and a cluster that
        // has just registered its default topic can still refuse it: both waits are state-gated.
        string createError = string.Empty;
        bool created = WaitUntil(() =>
        {
            try
            {
                admin.CreateTopic(MixAll.DefaultTopic, topic, 1);
                createError = string.Empty;
                return true;
            }
            catch (Exception e)
            {
                createError = e.GetType().Name + ": " + Clip(e.Message);
                return false;
            }
        }, CreateBudgetMs, PollIntervalMs * 4);
        if (!created)
        {
            problem = "creating " + topic + " kept failing: " + createError;
            return null;
        }

        TopicRouteData? route = null;
        bool readable = WaitUntil(() =>
        {
            route = client.GetTopicRouteData(topic);
            return route is not null && route.QueueDatas.Count > 0 && route.BrokerDatas.Count > 0;
        }, RouteBudgetMs, PollIntervalMs * 4);
        if (!readable || route is null)
        {
            problem = "the route of " + topic + " never became readable on "
                + string.Join(";", client.NameServerAddrs);
            return null;
        }

        string brokerName = route.QueueDatas[0].BrokerName;
        string addr = MasterAddrOf(route, brokerName);
        if (addr.Length == 0)
        {
            problem = "broker " + brokerName + " of topic " + topic
                + " has no master address in its own route";
            return null;
        }

        return new Ctx(admin, client, brokerName, addr, topic, group, ctrlGroup);
    }

    private static string MasterAddrOf(TopicRouteData route, string brokerName)
    {
        foreach (BrokerData bd in route.BrokerDatas)
        {
            if (bd.BrokerName == brokerName
                && bd.BrokerAddrs.TryGetValue(MixAll.MasterId, out string? master))
            {
                return master;
            }
        }

        return string.Empty;
    }

    // ---------------------------------------------------------------- entry point

    public static int Run(string[] args)
    {
        if (args.Length == 0)
        {
            PrintUsage();
            return 2;
        }

        string namesrv = args[0];
        if (!NamesrvLooksValid(namesrv))
        {
            Console.Error.WriteLine("invalid name server address: " + namesrv);
            PrintUsage();
            return 2;
        }

        if (!ParseLegs(args, out HashSet<string> legs))
        {
            PrintUsage();
            return 2;
        }

        if (args.Length > 2)
        {
            if (!long.TryParse(args[2], NumberStyles.None, CultureInfo.InvariantCulture, out long stamp)
                || stamp <= 0)
            {
                Console.Error.WriteLine("invalid stamp (unix seconds expected): " + args[2]);
                PrintUsage();
                return 2;
            }

            _stamp = N(stamp);
        }
        else
        {
            _stamp = N(DateTimeOffset.UtcNow.ToUnixTimeSeconds());
        }

        string topic = "PopModeCsTopic_" + _stamp;
        string group = "GID_PopModeCs_" + _stamp;
        string ctrlGroup = "GID_PopModeCsCtrl_" + _stamp;

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("SET_MESSAGE_REQUEST_MODE(401) + GET_ALL_MESSAGE_REQUEST_MODE(402) live"
            + " (C#): namesrv=" + namesrv + " stamp=" + _stamp + " legs=" + Join(legs.Order()));
        Console.WriteLine("  topic=" + topic + " group=" + group + " controlGroup=" + ctrlGroup);
        Console.WriteLine("  the subscription group is left to the broker's autoCreate, the topic is"
            + " created explicitly; every 401 goes to the master the topic route names");
        Console.WriteLine(new string('=', 70));

        var admin = new DefaultMQAdminExt("CsPopMode_" + _stamp);
        admin.SetNamesrvAddr(namesrv);
        admin.SetTimeoutMillis(RpcTimeoutMs);
        try
        {
            admin.Start();
        }
        catch (Exception e)
        {
            Check("L0 the admin started against " + namesrv, false, e.Message);
            Console.WriteLine("############ PASS=" + N(_pass) + " FAIL=" + N(_fail) + " ############");
            return 1;
        }

        int rc;
        try
        {
            MQClientInstance client = admin.Client();
            Ctx? ctx = Prepare(admin, client, topic, group, ctrlGroup, out string problem);
            if (ctx is null)
            {
                Check("L0 the topic was created and its route names the master broker 401 and 402"
                    + " are sent to", false, problem);
                rc = 1;
            }
            else
            {
                Check("L0 the topic was created and its route names the master broker 401 and 402"
                    + " are sent to", true, ctx.BrokerName + " master " + ctx.BrokerAddr);
                if (legs.Contains("l1")) LegPositive(ctx);
                if (legs.Contains("l2")) LegReplacement(ctx);
                if (legs.Contains("l3")) LegHeaderControl(ctx);
                rc = _fail == 0 ? 0 : 1;
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine("[FATAL] " + ex);
            ++_fail;
            rc = 1;
        }
        finally
        {
            // Best effort: a topic that outlives the run is broker bookkeeping, not a verdict on the
            // request shape, so a failed delete is a note and never a red run.
            try
            {
                admin.DeleteTopic(topic);
                Console.WriteLine("  [NOTE] cleanup: asked the cluster to delete topic " + topic);
            }
            catch (Exception e)
            {
                Console.WriteLine("  [NOTE] cleanup: deleting topic " + topic + " failed ("
                    + Clip(e.Message) + "); leaving it for the broker to expire");
            }

            try
            {
                admin.Shutdown();
            }
            catch (Exception)
            {
                // a cleanup failure does not change the conclusion of the assertions
            }

            Console.WriteLine("############ PASS=" + N(_pass) + " FAIL=" + N(_fail) + " ############");
        }

        return rc;
    }

    /// <summary>all | l1,l2,l3; nothing named means all three.</summary>
    private static bool ParseLegs(string[] args, out HashSet<string> legs)
    {
        legs = new HashSet<string>(StringComparer.Ordinal);
        if (args.Length > 1)
        {
            foreach (string leg in args[1].Split(',', StringSplitOptions.RemoveEmptyEntries
                       | StringSplitOptions.TrimEntries))
            {
                if (leg == "all")
                {
                    legs.Add("l1");
                    legs.Add("l2");
                    legs.Add("l3");
                }
                else if (leg is "l1" or "l2" or "l3")
                {
                    legs.Add(leg);
                }
                else
                {
                    Console.Error.WriteLine("unknown leg: " + leg);
                    return false;
                }
            }
        }

        if (legs.Count == 0)
        {
            legs.Add("l1");
            legs.Add("l2");
            legs.Add("l3");
        }

        return true;
    }

    /// <summary>Validated before anything is started, so a rejected argument never opens a
    /// connection.</summary>
    private static bool NamesrvLooksValid(string namesrv)
    {
        if (namesrv.Length == 0)
        {
            return false;
        }

        foreach (string entry in namesrv.Split(';', StringSplitOptions.RemoveEmptyEntries))
        {
            int colon = entry.LastIndexOf(':');
            if (colon <= 0 || colon == entry.Length - 1)
            {
                return false;
            }

            if (!int.TryParse(entry.AsSpan(colon + 1), NumberStyles.None, CultureInfo.InvariantCulture,
                    out int port) || port is < 1 or > 65535)
            {
                return false;
            }
        }

        return true;
    }

    private static void PrintUsage()
    {
        Console.Error.WriteLine(
            "usage: rmq pop-mode <namesrv> [legs] [stamp]"
            + "   e.g. rmq pop-mode 127.0.0.1:9876 all"
            + "   legs: all | l1,l2,l3   stamp: unix seconds (fresh one when omitted)");
    }
}
