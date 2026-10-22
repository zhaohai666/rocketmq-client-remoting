// Real-machine proof of BATCH_ACK_MESSAGE(200151) on the POP surface.
//
// Usage: rmq pop-batch-ack <namesrv> [legs] [stamp]
//        legs: all (default) or a comma list of l1,l2,l3
//        stamp: unix seconds for the resource names (a fresh one is generated when omitted)
//
//   dotnet run --no-build --project examples/RocketMQ.Examples -- pop-batch-ack 127.0.0.1:9876
//
// Why this leg needs to exist of its own: the POP consume path confirms one message at a time
// with ACK_MESSAGE(200051) and never assembles a batch, so no runtime path reaches the batch wire
// path. The merge key, the bit index, the packed payload and the "everything rides the JSON body,
// no custom header" request shape are reachable only through the primitive -- and a primitive
// nobody has pointed at a broker is a primitive that may not work.
//
// A return code cannot answer "which offsets did the broker really clear", because the body names
// them only as a start offset plus a bit set. So every leg's criterion is an observation window
// plus broker-side state:
//
//   L1 ONE REQUEST, WHOLE BATCH. Send 8 messages to a single-queue topic, wait until the queue
//      really holds 8, pop them all, and clear every checkpoint with a single 200151. The body is
//      read back before it is sent: exactly the popped offsets once each, one entry per merge key,
//      every entry echoing the checkpoint it came from, the broker name of the FIRST checkpoint,
//      the packed bit set decoding back to exactly the batch offsets, the bytes surviving the wire
//      codec, and the frame carrying no custom header. Then a silence window of 2.5x the
//      invisibility: a wrong offset or bit leaves a checkpoint alive and the revive service puts
//      that message back inside the window.
//   L2 PAIRED CONTROL. The same batch with exactly one checkpoint left out. Only that message may
//      come back -- the assertion that fails if a bit set is treated as "the whole batch". The
//      follow-up pins the retry direction: the revived checkpoints carry marker "1" and the batch
//      body that clears them names the BUSINESS topic, because the broker re-derives the physical
//      queue from marker + group.
//   L3 NO OVER-MERGING. Two pop rounds of one queue cleared by one request. The rounds must land in
//      different entries (retry@queueId@ckQueueOffset@popTime is the merge key); collapsing them
//      would acknowledge offsets nobody popped.
//
// The request code needs no broker switch and no restart: 200151 is registered unconditionally.
// The POP prerequisites are the ones the single-message path already needs -- timerWheelEnable,
// defaultMessageRequestMode=PULL and popResponseReturnActualRetryTopic=false.
//
// Which RPC answered a message is deliberately never the criterion: a POP of the business topic
// also consults the retry queue, and the client rewrites the reply topic to the requested one, so
// the topic name on a popped message proves nothing about the queue it came from. The evidence is
// the checkpoint's retry marker segment plus the %RETRY%<group>_<topic> route on the name server,
// and that route is probed only AFTER the ack: a route can take a broker heartbeat to appear, and
// no wait may push an ack past the invisibility of the checkpoint it is about to clear.
//
// Every window is gated on recorded state, never on a fixed sleep, and every wait carries an
// explicit budget (the transport's own default timeout is 15 s, so an implicit chain of waits
// could hang the run). A sighting is captured at the moment it is seen -- every POP that returns
// a message hides it again for the whole invisibility window -- and the probe stops at the first
// sighting plus a short tail so the ack lands while the collected checkpoint is still live.
//
// Final line: "############ PASS=<n> FAIL=<n> ############"; exit 0 with FAIL=0, 2 for bad
// arguments (nothing is started, so no connection is made), 1 for a fatal error.
using System.Globalization;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LivePopBatchAck
{
    // ---------------------------------------------------------------- windows and budgets

    /// <summary>Ten seconds of invisibility: far above the broker's 1 s revive scan, short enough
    /// for the whole run to stay in a few minutes.</summary>
    private const long InvisibleMs = 10000;

    /// <summary>A checkpoint no ack covered is revived at popTime + invisibleTime, so 2.5x that is
    /// a generous bound on "it should have come back by now".</summary>
    private const int SilenceWindowMs = 25000;

    /// <summary>L2's revival window is a probe, not a fixed window: it stops shortly after the
    /// dropped message is sighted, because the collected checkpoints must stay inside their own
    /// invisibility while the ack is assembled.</summary>
    private const int ReviveProbeBudgetMs = 40000;
    private const int ReviveTailMs = 3000;

    /// <summary>ConsumeQueue dispatch trails the send reply, and a route reaches the name server a
    /// heartbeat later; both waits are bounded and state-gated.</summary>
    private const int DepthBudgetMs = 30000;
    private const int RouteBudgetMs = 30000;
    private const int SendAttempts = 5;

    private const int RoundIntervalMs = 500;
    private const int OffsetRpcTimeoutMs = 10000;
    private const int BatchAckTimeoutMs = 10000;
    private const int PollOverheadMs = 5000;
    private const int BatchPopPollMs = 2000;
    private const int RoundPopMaxNums = 32;
    private const int AllQueues = -1;

    // ---------------------------------------------------------------- resources

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

    private static string Clip(string s) => s.Length > 240 ? s[..240] + "..." : s;

    private static string Join(IEnumerable<string> v) => "[" + string.Join(", ", v) + "]";

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    // ---------------------------------------------------------------- checkpoints

    /// <summary>The 8-segment POP_CK of one popped message, decoded into exactly the fields the
    /// batch body is built from.</summary>
    private sealed class Ck
    {
        public Ck(string raw, string body)
        {
            Raw = raw;
            Body = body;
        }

        public string Raw { get; }
        public string Body { get; }
        public long CkQueueOffset { get; set; }
        public long QueueOffset { get; set; }
        public long PopTime { get; set; }
        public long InvisibleTime { get; set; }
        public string Retry { get; set; } = string.Empty;
        public string BrokerName { get; set; } = string.Empty;
        public int QueueId { get; set; }
        public int ReviveQid { get; set; }

        /// <summary>What the builder groups on: two checkpoints with the same key are one entry.</summary>
        public string MergeKey => Retry + "@" + N(QueueId) + "@" + N(CkQueueOffset) + "@" + N(PopTime);

        public override string ToString() =>
            "{" + Body + " ck=" + N(CkQueueOffset) + " off=" + N(QueueOffset) + " r=" + Retry
            + " q=" + N(QueueId) + " rq=" + N(ReviveQid) + " pt=" + N(PopTime) + " it="
            + N(InvisibleTime) + " broker=" + BrokerName + "}";
    }

    private static string PopCkOf(MessageExt m) =>
        m.Properties.TryGetValue(MessageConst.PropertyPopCk, out string? v) ? v : string.Empty;

    private static string BodyOf(MessageExt m) => Encoding.UTF8.GetString(m.Body);

    private static string[] RawOf(IEnumerable<Ck> cks) => cks.Select(c => c.Raw).ToArray();

    /// <summary>Parses a checkpoint or says what is wrong with it -- a message that arrives without
    /// a readable CK cannot be named by a batch body at all.</summary>
    private static Ck? ParseCk(string raw, string body, out string problem)
    {
        problem = string.Empty;
        if (raw.Length == 0)
        {
            problem = body + ": no POP_CK on the message";
            return null;
        }

        try
        {
            string[] seg = ExtraInfoUtil.Split(raw);
            if (seg.Length != 8)
            {
                problem = body + ": not 8 segments (" + N(seg.Length) + "): '" + raw + "'";
                return null;
            }

            var ck = new Ck(raw, body)
            {
                CkQueueOffset = ExtraInfoUtil.GetCkQueueOffset(seg),
                PopTime = ExtraInfoUtil.GetPopTime(seg),
                InvisibleTime = ExtraInfoUtil.GetInvisibleTime(seg),
                ReviveQid = ExtraInfoUtil.GetReviveQid(seg),
                Retry = ExtraInfoUtil.GetRetry(seg),
                BrokerName = ExtraInfoUtil.GetBrokerName(seg),
                QueueId = ExtraInfoUtil.GetQueueId(seg),
                QueueOffset = ExtraInfoUtil.GetQueueOffset(seg),
            };
            return ck;
        }
        catch (Exception e)
        {
            problem = body + ": unreadable checkpoint '" + raw + "' (" + e.Message + ")";
            return null;
        }
    }

    private static (List<Ck> Cks, List<string> Problems) ParseAll(IEnumerable<MessageExt> msgs)
    {
        var cks = new List<Ck>();
        var problems = new List<string>();
        foreach (MessageExt m in msgs)
        {
            Ck? ck = ParseCk(PopCkOf(m), BodyOf(m), out string problem);
            if (ck is null) problems.Add(problem);
            else cks.Add(ck);
        }

        return (cks, problems);
    }

    /// <summary>First-seen checkpoint per merge key: exactly what the builder reads the entry's
    /// fields off.</summary>
    private static Dictionary<string, Ck> FirstByMergeKey(IEnumerable<Ck> cks)
    {
        var byKey = new Dictionary<string, Ck>(StringComparer.Ordinal);
        foreach (Ck ck in cks)
        {
            if (!byKey.ContainsKey(ck.MergeKey))
            {
                byKey[ck.MergeKey] = ck;
            }
        }

        return byKey;
    }

    private static List<string> DistinctKeys(IEnumerable<Ck> cks) =>
        cks.Select(c => c.MergeKey).Distinct().OrderBy(k => k, StringComparer.Ordinal).ToList();

    private static List<long> OffsetsOf(IEnumerable<Ck> cks) =>
        cks.Select(c => c.QueueOffset).OrderBy(o => o).ToList();

    // ---------------------------------------------------------------- the batch body

    /// <summary>The same merge key computed off an entry instead of off a checkpoint: an entry may
    /// only ever claim a key one of its checkpoints carried.</summary>
    private static string KeyOf(BatchAck ack) =>
        ack.Retry + "@" + N(ack.QueueId) + "@" + N(ack.StartOffset) + "@" + N(ack.PopTime);

    private static string PackedBitsOf(JsonValue entry) => entry.Get("b").StringValue();

    /// <summary>The consume-queue offsets one entry clears, read off the wire bytes: start offset
    /// plus every bit that is set in the packed payload.</summary>
    private static List<long> OffsetsOfPackedBits(string base64, long startOffset)
    {
        var offsets = new List<long>();
        if (base64.Length == 0)
        {
            return offsets;
        }

        byte[] packed = Convert.FromBase64String(base64);
        for (int i = 0; i < packed.Length * 8; ++i)
        {
            if ((packed[i / 8] & (1 << (i % 8))) != 0)
            {
                offsets.Add(startOffset + i);
            }
        }

        return offsets;
    }

    private static List<long> AckedOffsets(BatchAck ack) =>
        ack.AckOffsets.Select(i => ack.StartOffset + i).OrderBy(o => o).ToList();

    private static bool SameOffsets(List<long> got, List<long> want) =>
        got.Count == want.Count && got.Zip(want).All(pair => pair.First == pair.Second);

    private static string Dump(List<long> offsets) =>
        "[" + string.Join(",", offsets.Select(o => N(o))) + "]";

    // ---------------------------------------------------------------- the wire per leg

    /// <summary>One single-queue topic plus the POP primitives aimed straight at the broker that
    /// owns it, so no leg depends on the client's own route choice.</summary>
    private sealed class Wire
    {
        public Wire(MQClientInstance client, string topic, string brokerName, string addr)
        {
            Client = client;
            Topic = topic;
            BrokerName = brokerName;
            Addr = addr;
        }

        public MQClientInstance Client { get; }
        public string Topic { get; }
        public string BrokerName { get; }
        public string Addr { get; }

        public PopResult Pop(string group, string topic, int queueId, int maxNums, long pollMs) =>
            Client.PopMessage(group, topic, queueId, maxNums, InvisibleMs, pollMs, 0, "*", "TAG",
                false, (int)(pollMs + PollOverheadMs), BrokerName, Addr);

        public long Depth()
        {
            var mq = new MessageQueue(Topic, BrokerName, 0);
            return Client.GetMaxOffset(mq, OffsetRpcTimeoutMs, Addr)
                   - Client.GetMinOffset(mq, OffsetRpcTimeoutMs, Addr);
        }
    }

    // ---------------------------------------------------------------- observation

    private sealed class Sighting
    {
        public Sighting(string body, string topic, string ck)
        {
            Body = body;
            Topic = topic;
            Ck = ck;
        }

        public string Body { get; }
        public string Topic { get; }
        public string Ck { get; }
    }

    /// <summary>Everything a window of POP rounds answered. Checkpoints are recorded at sighting
    /// time, because a re-pop of the same body answers nothing once the message is invisible again.
    /// The topic a sighting is recorded under is bookkeeping only: it is never evidence about which
    /// queue answered.</summary>
    private sealed class Eyes
    {
        public List<Sighting> Sightings { get; } = new();
        public List<string> Notes { get; } = new();

        public bool CameBack() => Sightings.Count > 0;

        public bool CameBack(string body) => Sightings.Exists(s => s.Body == body);

        public List<string> Bodies() =>
            Sightings.Select(s => s.Body).Distinct().OrderBy(b => b, StringComparer.Ordinal).ToList();

        public List<Sighting> SightingsOf(string body) =>
            Sightings.FindAll(s => s.Body == body);

        public List<string> TopicsSeen() =>
            Sightings.Select(s => s.Topic).Distinct().OrderBy(t => t, StringComparer.Ordinal).ToList();

        public string Summary() => CameBack()
            ? "sighted=" + Join(Bodies()) + " notes=" + Join(Notes)
            : (Notes.Count > 0 ? "nothing sighted, notes=" + Join(Notes) : "nothing sighted");
    }

    /// <summary>One round pops the business topic AND the group's POP retry topic: a checkpoint
    /// nobody acked is revived into the retry topic, and a POP of the business topic only consults
    /// that queue by probability, so watching one of the two would make a silence ambiguous.</summary>
    private static void Round(Wire wire, string group, Eyes eyes)
    {
        foreach (string topic in new[] { wire.Topic, ExtraInfoUtil.BuildPopRetryTopicV1(wire.Topic, group) })
        {
            PopResult res;
            try
            {
                res = wire.Pop(group, topic, AllQueues, RoundPopMaxNums, 0);
            }
            catch (MQBrokerException e) when (e.ResponseCode == ResponseCode.TopicNotExist)
            {
                // A POP retry topic only exists once the revive service has put something in it.
                continue;
            }
            catch (Exception e)
            {
                eyes.Notes.Add(topic + ": " + e.Message);
                continue;
            }

            if (!res.IsFound)
            {
                continue;
            }

            foreach (MessageExt m in res.MsgFoundList)
            {
                eyes.Sightings.Add(new Sighting(BodyOf(m), topic, PopCkOf(m)));
            }
        }
    }

    /// <summary>The expected answer is silence, so this one runs the whole window.</summary>
    private static void Watch(Wire wire, string group, int windowMs, Eyes eyes)
    {
        long deadline = NowMs() + windowMs;
        while (NowMs() < deadline)
        {
            Round(wire, group, eyes);
            Thread.Sleep(RoundIntervalMs);
        }
    }

    /// <summary>Pops until the wanted body turns up, keeps going for a short tail so the whole
    /// revival gets recorded, then stops and returns whether it came back. Stopping at the first
    /// answer would judge the acked messages over fewer rounds than the revived one; running on
    /// would let the collected checkpoint expire before an ack can take it.</summary>
    private static bool Probe(Wire wire, string group, string waitFor, Eyes eyes,
        int budgetMs, int tailMs)
    {
        long deadline = NowMs() + budgetMs;
        long tailUntil = 0;
        while (NowMs() < deadline)
        {
            Round(wire, group, eyes);
            if (tailUntil == 0 && eyes.CameBack(waitFor))
            {
                tailUntil = NowMs() + tailMs;
            }

            if (tailUntil != 0 && NowMs() >= tailUntil)
            {
                return true;
            }

            Thread.Sleep(RoundIntervalMs);
        }

        return eyes.CameBack(waitFor);
    }

    // ---------------------------------------------------------------- setup helpers

    /// <summary>Waits for the route of a freshly created topic, bounded.</summary>
    private static TopicRouteData? WaitRoute(MQClientInstance client, string topic, int budgetMs)
    {
        long deadline = NowMs() + budgetMs;
        while (NowMs() < deadline)
        {
            TopicRouteData? route = client.GetTopicRouteData(topic);
            if (route is not null && route.BrokerDatas.Count > 0)
            {
                return route;
            }

            Thread.Sleep(1000);
        }

        return client.GetTopicRouteData(topic);
    }

    private static bool RouteVisible(MQClientInstance client, string topic, int budgetMs) =>
        WaitRoute(client, topic, budgetMs) is not null;

    /// <summary>Creates a single-queue topic, waits for its route and sends one message per body,
    /// retrying a send while the route is still propagating.</summary>
    private static (string BrokerName, string Addr)? PrepareTopic(DefaultMQProducer producer,
        MQClientInstance client, string topic, IEnumerable<string> bodies)
    {
        try
        {
            producer.CreateTopic(MixAll.DefaultTopic, topic, 1);
        }
        catch (Exception e)
        {
            Console.WriteLine("  creating " + topic + " failed: " + e.Message);
            return null;
        }

        TopicRouteData? route = WaitRoute(client, topic, RouteBudgetMs);
        if (route is null || route.BrokerDatas.Count == 0)
        {
            return null;
        }

        string brokerName = route.BrokerDatas[0].BrokerName;
        string addr = MQClientInstance.FindBrokerAddrInRoute(route, brokerName);
        if (addr.Length == 0)
        {
            return null;
        }

        foreach (string body in bodies)
        {
            bool sent = false;
            for (int attempt = 0; attempt < SendAttempts && !sent; ++attempt)
            {
                try
                {
                    var msg = new Message(topic, "TagBa", "pk-" + body, Encoding.UTF8.GetBytes(body));
                    producer.Send(msg);
                    sent = true;
                }
                catch (Exception)
                {
                    Thread.Sleep(2000);
                }
            }

            if (!sent)
            {
                throw new InvalidOperationException("send failed for " + body + " on " + topic);
            }
        }

        return (brokerName, addr);
    }

    /// <summary>Messages reach the consume queue asynchronously: a partially dispatched batch would
    /// make "one request cleared the batch" unprovable, so the depth is waited for, bounded.</summary>
    private static long WaitDepth(Wire wire, int want)
    {
        long deadline = NowMs() + DepthBudgetMs;
        long depth = 0;
        while (NowMs() < deadline)
        {
            depth = wire.Depth();
            if (depth >= want)
            {
                return depth;
            }

            Thread.Sleep(1000);
        }

        return wire.Depth();
    }

    // ---------------------------------------------------------------- L1

    private static void LegAll(DefaultMQProducer producer, MQClientInstance client)
    {
        const int Batch = 8;
        string topic = "BaAll_" + _stamp;
        string group = "GID_BaAll_" + _stamp;
        var bodies = Enumerable.Range(0, Batch).Select(i => "ba-all-" + N(i)).ToList();

        Console.WriteLine("=== L1 one 200151 request clears a whole batch (" + topic + ") ===");
        (string BrokerName, string Addr)? prepared;
        try
        {
            prepared = PrepareTopic(producer, client, topic, bodies);
        }
        catch (Exception e)
        {
            Check("L1 sent 8 messages to a single-queue topic", false, e.Message);
            return;
        }

        if (prepared is null)
        {
            Check("L1 the topic route is usable", false, topic + " has no publish address");
            return;
        }

        var wire = new Wire(client, topic, prepared.Value.BrokerName, prepared.Value.Addr);
        Check("L1 sent 8 messages to a single-queue topic", true,
            topic + " @ " + wire.BrokerName + " " + wire.Addr);

        long depth = WaitDepth(wire, Batch);
        Check("L1 all 8 messages are on the queue before popping", depth >= Batch,
            "depth=" + N(depth) + " want=" + N(Batch));
        if (depth < Batch)
        {
            return;
        }

        PopResult res;
        try
        {
            res = wire.Pop(group, topic, 0, RoundPopMaxNums, BatchPopPollMs);
        }
        catch (Exception e)
        {
            Check("L1 POP_MESSAGE answered the whole batch", false, e.Message);
            return;
        }

        Check("L1 POP_MESSAGE answered the whole batch", res.IsFound && res.MsgFoundList.Count == Batch,
            "status=" + PopStatusNames.Name(res.Status) + " got=" + N(res.MsgFoundList.Count));
        if (res.MsgFoundList.Count != Batch)
        {
            return;
        }

        (List<Ck> cks, List<string> problems) = ParseAll(res.MsgFoundList);
        Check("L1 every message carries a parsable 8-segment POP_CK", problems.Count == 0,
            string.Join("; ", problems));
        Check("L1 the batch has one checkpoint per message", cks.Count == Batch, N(cks.Count));
        if (cks.Count != Batch)
        {
            return;
        }

        // Everything below trusts these: the body can only name a message through the checkpoint's
        // offset segment and can only be routed through its marker, broker name and queueId.
        string shapeProblem = string.Empty;
        foreach (Ck ck in cks)
        {
            if (ck.Retry != ExtraInfoUtil.NormalTopic || ck.BrokerName != wire.BrokerName
                || ck.QueueId != 0 || ck.InvisibleTime != InvisibleMs || ck.PopTime <= 0
                || ck.CkQueueOffset > ck.QueueOffset)
            {
                shapeProblem = ck.ToString();
                break;
            }
        }

        Check("L1 the checkpoint fields the body is built from are the ones the primitive reads",
            shapeProblem.Length == 0, shapeProblem.Length == 0
                ? "ckOffset=" + N(cks[0].CkQueueOffset) + " popTime=" + N(cks[0].PopTime)
                    + " reviveQid=" + N(cks[0].ReviveQid)
                : "unexpected " + shapeProblem);
        if (shapeProblem.Length > 0)
        {
            return;
        }

        List<long> want = OffsetsOf(cks);
        List<string> keys = DistinctKeys(cks);
        string[] raws = RawOf(cks);

        BatchAckMessageRequestBody body;
        try
        {
            body = BatchAckMessageRequestBody.Build(group, topic, raws);
        }
        catch (Exception e)
        {
            Check("L1 the batch body is built from the popped checkpoints", false, e.Message);
            return;
        }

        Check("L1 body acknowledges exactly the popped offsets, once each",
            body.Acks.Count == 1 && SameOffsets(AckedOffsets(body.Acks[0]), want),
            "acked=" + Dump(body.Acks.Count > 0 ? AckedOffsets(body.Acks[0]) : new List<long>())
                + " want=" + Dump(want));

        Check("L1 body has exactly one entry per merge key", body.Acks.Count == keys.Count,
            N(body.Acks.Count) + " entries for " + keys.Count + " keys " + Join(keys));

        Dictionary<string, Ck> byKey = FirstByMergeKey(cks);
        string fieldProblem = string.Empty;
        foreach (BatchAck ack in body.Acks)
        {
            if (body.BrokerName != cks[0].BrokerName || ack.ConsumerGroup != group || ack.Topic != topic)
            {
                fieldProblem = "entry c='" + ack.ConsumerGroup + "' t='" + ack.Topic
                    + "' bodyBroker='" + body.BrokerName + "' want c='" + group + "' t='" + topic
                    + "' broker='" + cks[0].BrokerName + "' (topic broker '" + wire.BrokerName + "')";
                break;
            }

            if (!byKey.TryGetValue(KeyOf(ack), out Ck? source))
            {
                fieldProblem = "entry '" + KeyOf(ack) + "' matches no checkpoint";
                break;
            }

            if (ack.StartOffset != source.CkQueueOffset || ack.QueueId != source.QueueId
                || ack.ReviveQueueId != source.ReviveQid || ack.PopTime != source.PopTime
                || ack.InvisibleTime != source.InvisibleTime || ack.Retry != source.Retry
                || ack.AckOffsets.Count == 0)
            {
                fieldProblem = "entry " + KeyOf(ack) + " diverges from " + source
                    + ": so=" + N(ack.StartOffset) + " q=" + N(ack.QueueId) + " rq="
                    + N(ack.ReviveQueueId) + " pt=" + N(ack.PopTime) + " it=" + N(ack.InvisibleTime)
                    + " r='" + ack.Retry + "' bits=" + N(ack.AckOffsets.Count);
                break;
            }
        }

        Check("L1 every entry echoes its checkpoint fields, names the group, the business topic "
            + "and the FIRST checkpoint's broker name", fieldProblem.Length == 0, fieldProblem);

        // The wire form is where a packed bit set goes wrong silently: a non-minimal or reversed
        // payload still decodes as *some* set of offsets, so the offsets are decoded back out of
        // the bytes rather than trusted from the model.
        string encodedText = Encoding.UTF8.GetString(body.Encode());
        bool parses = Json.TryParse(encodedText, out JsonValue json, out string? parseError)
                      && json.Get("acks").IsArray
                      && json.Get("acks").Size() == body.Acks.Count;
        if (!parses)
        {
            Check("L1 body round-trips through the wire codec", false,
                parseError is { Length: > 0 } ? parseError! : "acks missing from " + Clip(encodedText));
            return;
        }

        JsonValue first = json.Get("acks").At(0);
        string packed = PackedBitsOf(first);
        byte[] packedBytes = Convert.FromBase64String(packed);
        Check("L1 bit set decodes back to exactly the batch offsets",
            SameOffsets(OffsetsOfPackedBits(packed, first.Get("so").IntValue()), want),
            "b='" + packed + "' decoded=" + Dump(OffsetsOfPackedBits(packed, first.Get("so").IntValue())));

        int highest = (int)(want[^1] - first.Get("so").IntValue());
        Check("L1 bit set is byte-minimal little-endian base64",
            packedBytes.Length > 0 && packedBytes.Length == highest / 8 + 1
            && packedBytes[^1] != 0,
            "bytes=" + N(packedBytes.Length) + " highestBit=" + N(highest));

        Check("L1 entries use the compact key spellings and the body names the first broker",
            first.ObjectItems().Select(kv => kv.Key).SequenceEqual(
                new[] { "c", "t", "r", "so", "q", "rq", "pt", "it", "b" })
            && json.Get("brokerName").StringValue() == cks[0].BrokerName
            && first.Get("t").StringValue() == topic && first.Get("c").StringValue() == group,
            Clip(Json.Dump(first)));

        bool decoded = BatchAckMessageRequestBody.Decode(body.Encode(), out BatchAckMessageRequestBody back)
                       && back.BrokerName == body.BrokerName
                       && back.Acks.Count == body.Acks.Count
                       && SameOffsets(back.Acks.SelectMany(AckedOffsets).OrderBy(o => o).ToList(), want);
        Check("L1 body round-trips through the wire codec", decoded,
            "bytes=" + N(body.Encode().Length) + " " + Clip(encodedText));

        // The frame as it goes out: request code 200151, everything inside the body, no header.
        var frame = RemotingCommand.CreateRequestCommand(RequestCode.BatchAckMessage, null);
        frame.Body = body.Encode();
        RemotingCommand readBack = RemotingCommand.Decode(frame.Encode());
        Check("L1 request frame carries 200151 with the body and no custom header",
            readBack.Code == RequestCode.BatchAckMessage && readBack.ExtFields.Count == 0
            && readBack.Body.Length == frame.Body.Length,
            "code=" + N(readBack.Code) + " extFields=" + N(readBack.ExtFields.Count)
                + " bodyBytes=" + N(readBack.Body.Length));

        int code;
        try
        {
            // The address is left to the primitive: it resolves the broker from the FIRST
            // checkpoint of the body, which is the routing half of the live proof.
            code = client.BatchAckMessage(group, topic, raws, BatchAckTimeoutMs);
        }
        catch (Exception e)
        {
            Check("L1 one BATCH_ACK_MESSAGE answered SUCCESS", false, e.Message);
            return;
        }

        Check("L1 one BATCH_ACK_MESSAGE answered SUCCESS", code == ResponseCode.Success,
            "code=" + N(code) + " for " + N(cks.Count) + " checkpoints in " + N(body.Acks.Count)
                + " entries");

        var eyes = new Eyes();
        Watch(wire, group, SilenceWindowMs, eyes);
        Check("L1 nothing revived inside 2.5x the invisibility window (the batch really landed)",
            !eyes.CameBack(), eyes.Summary());
    }

    // ---------------------------------------------------------------- L2

    private static void LegPartial(DefaultMQProducer producer, MQClientInstance client)
    {
        const int Batch = 6;
        string topic = "BaPart_" + _stamp;
        string group = "GID_BaPart_" + _stamp;
        var bodies = Enumerable.Range(0, Batch).Select(i => "ba-part-" + N(i)).ToList();

        Console.WriteLine("=== L2 paired control: one checkpoint left out of the batch ("
            + topic + ") ===");
        (string BrokerName, string Addr)? prepared;
        try
        {
            prepared = PrepareTopic(producer, client, topic, bodies);
        }
        catch (Exception e)
        {
            Check("L2 sent 6 messages to a single-queue topic", false, e.Message);
            return;
        }

        if (prepared is null)
        {
            Check("L2 the topic route is usable", false, topic + " has no publish address");
            return;
        }

        var wire = new Wire(client, topic, prepared.Value.BrokerName, prepared.Value.Addr);
        Check("L2 sent 6 messages to a single-queue topic", true,
            topic + " @ " + wire.BrokerName + " " + wire.Addr);

        long depth = WaitDepth(wire, Batch);
        Check("L2 all 6 messages are on the queue before popping", depth >= Batch,
            "depth=" + N(depth) + " want=" + N(Batch));
        if (depth < Batch)
        {
            return;
        }

        PopResult res;
        try
        {
            res = wire.Pop(group, topic, 0, RoundPopMaxNums, BatchPopPollMs);
        }
        catch (Exception e)
        {
            Check("L2 POP_MESSAGE answered the batch", false, e.Message);
            return;
        }

        Check("L2 POP_MESSAGE answered the batch", res.IsFound && res.MsgFoundList.Count == Batch,
            "status=" + PopStatusNames.Name(res.Status) + " got=" + N(res.MsgFoundList.Count));
        if (res.MsgFoundList.Count != Batch)
        {
            return;
        }

        (List<Ck> cks, List<string> problems) = ParseAll(res.MsgFoundList);
        Check("L2 every checkpoint is parsable", problems.Count == 0, string.Join("; ", problems));
        if (cks.Count != Batch)
        {
            return;
        }

        // The dropped message is the highest offset: the last bit the payload can carry.
        List<Ck> sorted = cks.OrderBy(c => c.QueueOffset).ToList();
        Ck dropped = sorted[^1];
        List<Ck> kept = sorted[..^1];
        List<string> keptBodies = kept.Select(c => c.Body).ToList();

        BatchAckMessageRequestBody partial;
        try
        {
            partial = BatchAckMessageRequestBody.Build(group, topic, RawOf(kept));
        }
        catch (Exception e)
        {
            Check("L2 the partial body is built from 5 checkpoints", false, e.Message);
            return;
        }

        List<long> partialOffsets = partial.Acks.SelectMany(AckedOffsets).OrderBy(o => o).ToList();
        Check("L2 body acknowledges 5 of the 6 offsets and not the dropped one",
            SameOffsets(partialOffsets, OffsetsOf(kept)) && !partialOffsets.Contains(dropped.QueueOffset),
            "acked=" + Dump(partialOffsets) + " dropped=" + N(dropped.QueueOffset));

        int first;
        try
        {
            first = client.BatchAckMessage(group, topic, RawOf(kept), BatchAckTimeoutMs);
        }
        catch (Exception e)
        {
            Check("L2 partial BATCH_ACK_MESSAGE answered SUCCESS", false, e.Message);
            return;
        }

        Check("L2 partial BATCH_ACK_MESSAGE answered SUCCESS", first == ResponseCode.Success,
            "code=" + N(first) + " for " + N(kept.Count) + " of " + N(cks.Count) + " checkpoints");

        var eyes = new Eyes();
        bool broughtBack = Probe(wire, group, dropped.Body, eyes, ReviveProbeBudgetMs, ReviveTailMs);

        // THE control: if a bit set were decorative -- on the client or on the broker -- this leg
        // would look exactly like L1.
        Check("L2 the unacked message really came back", broughtBack,
            "want='" + dropped.Body + "' sighted=" + Join(eyes.Bodies()));
        List<string> ackedThatReturned = eyes.Bodies().Where(keptBodies.Contains).ToList();
        Check("L2 the five acked messages stayed gone while the dropped one came back",
            broughtBack && ackedThatReturned.Count == 0
            && eyes.Bodies().SequenceEqual(new List<string> { dropped.Body }),
            "sighted=" + Join(eyes.Bodies()) + " kept=" + Join(keptBodies));
        if (!broughtBack)
        {
            Console.WriteLine("     watch notes: " + Join(eyes.Notes));
            return;
        }

        List<Ck> revived = new();
        var revivedProblems = new List<string>();
        foreach (Sighting s in eyes.SightingsOf(dropped.Body))
        {
            Ck? ck = ParseCk(s.Ck, s.Body, out string problem);
            if (ck is null) revivedProblems.Add(problem);
            else revived.Add(ck);
        }

        Check("L2 the revived checkpoints are parsable", revivedProblems.Count == 0,
            string.Join("; ", revivedProblems));
        if (revived.Count == 0)
        {
            return;
        }

        List<string> markers = revived.Select(c => c.Retry).Distinct().ToList();
        Check("L2 the revived checkpoints carry the retry marker (they live in the retry queue)",
            markers.Count == 1 && markers[0] == ExtraInfoUtil.RetryTopic,
            "markers=" + Join(markers) + " sightings=" + N(revived.Count));

        // Cleared right away: the invisibility of these checkpoints started when the reply handed
        // them over, and everything between sighting and ack has to be a local check.
        string[] retryRaws = RawOf(revived);
        BatchAckMessageRequestBody retryBody;
        try
        {
            retryBody = BatchAckMessageRequestBody.Build(group, topic, retryRaws);
        }
        catch (Exception e)
        {
            Check("L2 the retry body is built from the revived checkpoints", false, e.Message);
            return;
        }

        // The body names the BUSINESS topic although the checkpoints came out of the retry topic:
        // the broker re-derives the physical topic from marker + group, so a client that passed the
        // retry topic here would double-wrap the name and ack nothing.
        Check("L2 the retry body keeps the business topic and carries the marker on every entry",
            retryBody.Acks.Count >= 1 && retryBody.Acks.All(a =>
                a.Topic == topic && a.Retry == ExtraInfoUtil.RetryTopic && a.ConsumerGroup == group),
            "entries=" + N(retryBody.Acks.Count) + " t='" + topic + "' r="
                + Join(retryBody.Acks.Select(a => a.Retry)));

        int second;
        try
        {
            second = client.BatchAckMessage(group, topic, retryRaws, BatchAckTimeoutMs);
        }
        catch (Exception e)
        {
            Check("L2 retry BATCH_ACK_MESSAGE answered SUCCESS", false, e.Message);
            return;
        }

        Check("L2 retry BATCH_ACK_MESSAGE answered SUCCESS", second == ResponseCode.Success,
            "code=" + N(second) + " for " + N(revived.Count) + " revived checkpoints in "
                + N(retryBody.Acks.Count) + " entries");

        // Broker state, read only after the ack so that this wait cannot delay it: the POP retry
        // topic really has a route, which is what says the revive service moved the message there.
        // Which topic a sighting was recorded under is printed as context only -- a POP of the
        // business topic also consults the retry queue and the reply topic is rewritten, so it can
        // name either queue for the same physical revival and proves nothing.
        string retryTopic = ExtraInfoUtil.BuildPopRetryTopicV1(topic, group);
        Check("L2 the POP retry topic really exists on the name server",
            RouteVisible(client, retryTopic, RouteBudgetMs),
            retryTopic + " (sightings recorded under " + Join(eyes.TopicsSeen()) + ")");

        var settled = new Eyes();
        Watch(wire, group, SilenceWindowMs, settled);
        Check("L2 the marker-driven retry ack silenced it (no further revival)",
            !settled.CameBack(), settled.Summary());
    }

    // ---------------------------------------------------------------- L3

    private static void LegRounds(DefaultMQProducer producer, MQClientInstance client)
    {
        const int Batch = 4;
        const int PerRound = 2;
        string topic = "BaRounds_" + _stamp;
        string group = "GID_BaRounds_" + _stamp;
        var bodies = Enumerable.Range(0, Batch).Select(i => "ba-rounds-" + N(i)).ToList();

        Console.WriteLine("=== L3 two pop rounds of one queue, one batch request (" + topic + ") ===");
        (string BrokerName, string Addr)? prepared;
        try
        {
            prepared = PrepareTopic(producer, client, topic, bodies);
        }
        catch (Exception e)
        {
            Check("L3 sent 4 messages to a single-queue topic", false, e.Message);
            return;
        }

        if (prepared is null)
        {
            Check("L3 the topic route is usable", false, topic + " has no publish address");
            return;
        }

        var wire = new Wire(client, topic, prepared.Value.BrokerName, prepared.Value.Addr);
        Check("L3 sent 4 messages to a single-queue topic", true,
            topic + " @ " + wire.BrokerName + " " + wire.Addr);

        long depth = WaitDepth(wire, Batch);
        Check("L3 all 4 messages are on the queue before popping", depth >= Batch,
            "depth=" + N(depth) + " want=" + N(Batch));
        if (depth < Batch)
        {
            return;
        }

        var rounds = new List<List<Ck>>();
        for (int round = 0; round < 2; ++round)
        {
            PopResult res;
            try
            {
                res = wire.Pop(group, topic, 0, PerRound, BatchPopPollMs);
            }
            catch (Exception e)
            {
                Check("L3 POP round " + N(round) + " answers 2 messages", false, e.Message);
                return;
            }

            (List<Ck> cks, List<string> problems) = ParseAll(res.MsgFoundList);
            Check("L3 POP round " + N(round) + " answers 2 messages with parsable checkpoints",
                res.IsFound && res.MsgFoundList.Count == PerRound && problems.Count == 0,
                "status=" + PopStatusNames.Name(res.Status) + " got=" + N(res.MsgFoundList.Count)
                    + " " + string.Join("; ", problems));
            if (res.MsgFoundList.Count != PerRound || problems.Count > 0)
            {
                return;
            }

            rounds.Add(cks);
        }

        List<Ck> all = rounds.SelectMany(r => r).ToList();
        List<long> offsets = OffsetsOf(all);
        Check("L3 the two rounds are disjoint batches of one queue",
            offsets.Distinct().Count() == Batch
            && rounds[0].All(c => c.QueueId == 0) && rounds[1].All(c => c.QueueId == 0),
            "offsets=" + Dump(offsets));

        List<string> keys = DistinctKeys(all);
        Check("L3 the two rounds produce different merge keys, so the body cannot collapse them",
            keys.Count == 2, Join(keys));

        BatchAckMessageRequestBody body;
        try
        {
            body = BatchAckMessageRequestBody.Build(group, topic, RawOf(all));
        }
        catch (Exception e)
        {
            Check("L3 one body is built from both rounds", false, e.Message);
            return;
        }

        Check("L3 one body covers all 4 offsets exactly once in 2 entries",
            body.Acks.Count == 2
            && SameOffsets(body.Acks.SelectMany(AckedOffsets).OrderBy(o => o).ToList(), offsets),
            "entries=" + N(body.Acks.Count) + " acked="
                + Dump(body.Acks.SelectMany(AckedOffsets).OrderBy(o => o).ToList()));

        int code;
        try
        {
            code = client.BatchAckMessage(group, topic, RawOf(all), BatchAckTimeoutMs);
        }
        catch (Exception e)
        {
            Check("L3 one BATCH_ACK_MESSAGE for two rounds answered SUCCESS", false, e.Message);
            return;
        }

        Check("L3 one BATCH_ACK_MESSAGE for two rounds answered SUCCESS",
            code == ResponseCode.Success,
            "code=" + N(code) + " for " + N(all.Count) + " checkpoints in " + N(body.Acks.Count)
                + " entries");

        var eyes = new Eyes();
        Watch(wire, group, SilenceWindowMs, eyes);
        Check("L3 no message of either round revived inside 2.5x the window",
            !eyes.CameBack(), eyes.Summary());
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

        HashSet<string> legs = new(StringComparer.Ordinal);
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
                    PrintUsage();
                    return 2;
                }
            }
        }

        if (legs.Count == 0)
        {
            legs.Add("l1");
            legs.Add("l2");
            legs.Add("l3");
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

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("BATCH_ACK_MESSAGE(200151) live (C#): namesrv=" + namesrv
            + " stamp=" + _stamp + " invisibleTime=" + N(InvisibleMs) + "ms silence="
            + N(SilenceWindowMs) + "ms legs=" + Join(legs.Order()));
        Console.WriteLine("  200151 is registered unconditionally, no broker switch and no restart;"
            + " the POP prerequisites are timerWheelEnable, defaultMessageRequestMode=PULL and"
            + " popResponseReturnActualRetryTopic=false");
        Console.WriteLine(new string('=', 70));

        var producer = new DefaultMQProducer("PG_BaLive_" + _stamp);
        producer.NamesrvAddr = namesrv;
        producer.Start();
        MQClientInstance client = producer.Client();
        int rc;
        try
        {
            if (legs.Contains("l1")) LegAll(producer, client);
            if (legs.Contains("l2")) LegPartial(producer, client);
            if (legs.Contains("l3")) LegRounds(producer, client);
            rc = _fail == 0 ? 0 : 1;
        }
        catch (Exception ex)
        {
            Console.WriteLine("[FATAL] " + ex);
            ++_fail;
            rc = 1;
        }
        finally
        {
            try
            {
                producer.Shutdown();
            }
            catch (Exception)
            {
                // a cleanup failure does not change the conclusion of the assertions
            }

            Console.WriteLine("############ PASS=" + N(_pass) + " FAIL=" + N(_fail) + " ############");
        }

        return rc;
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
            "usage: rmq pop-batch-ack <namesrv> [legs] [stamp]"
            + "   e.g. rmq pop-batch-ack 127.0.0.1:9876 all"
            + "   legs: all | l1,l2,l3   stamp: unix seconds (fresh one when omitted)");
    }
}
