// Lite pull consumer (DefaultLitePullConsumer) live verification.
// Usage: rmq lite-pull [namesrv]
//
// Scenarios S1-S8: rebalance, subscribe/assign, poll, auto and manual commit, seek, offset recovery.
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

    /// <summary>Drains the buffer by **time**: when a strategy is assigned only part of the queues the message count is unknown, so we cannot wait by count.</summary>
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

    /// <summary>Queue list -> key set ("brokerName#queueId"), for cross-language comparison of assignment results.</summary>
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

    /// <summary>Message list -> body set; when onlyOnQueues is given, counts only messages from those queues.</summary>
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

    /// <summary>Waits for both instances to **fully split** the queues (assignment convergence takes one round of heartbeat + rebalance on each side).</summary>
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

    /// <summary>Installs strategy + subscription on the lite consumer (not started). Scenarios S7d-S7f all share this config surface.</summary>
    private static void SetupConsumer(DefaultLitePullConsumer c, string namesrv, string instanceName,
        IAllocateMessageQueueStrategy strategy)
    {
        c.SetNamesrvAddr(namesrv);
        c.SetInstanceName(instanceName);
        c.SetPollTimeoutMillis(1000);
        // Stock messages were all produced back in S2; a new group defaults to LAST and would skip them -> nothing received.
        // What we verify here is "the strategy really drives consumption", so consume from the queue head.
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
    /// An "everyone in the same machine room" resolver: the real cluster has only one broker, so queues and clients are all recorded in the same room,
    /// hence NEARBY necessarily takes the "own room" branch, equivalent to the inner strategy. It also keeps call records -
    /// proving rebalance really asked each queue and client for its room, instead of the strategy being renamed yet never participating in assignment.
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

    /// <summary>Independent probe reading the broker offset for this slot; QUERY_NOT_FOUND (never committed) is recorded as -1.</summary>
    private static long BrokerOffset(DefaultMQPullConsumer probe, MessageQueue mq) =>
        probe.FetchConsumeOffset(mq, out long off) ? off : -1;

    private static void ThreeOffsetTable(string namesrv, IReadOnlyList<MessageQueue> foreignPool)
    {
        string offTopic = "LiteOffNet_" + Stamp;
        string offGroup = "LiteOffPGNet_" + Stamp;
        const int NBig = 1200;   // > the poll single-shot cap of 1024 => one delivery is bound to leave an undelivered tail

        List<MessageQueue> queues = new();
        int landed = 0;
        {
            var prep = new DefaultMQProducer("PG_PrepareOffNet_" + Stamp);
            prep.NamesrvAddr = namesrv;
            prep.Start();
            try
            {
                prep.CreateTopic("TBW102", offTopic, 1);
            }
            catch (Exception e)
            {
                Console.WriteLine("!! CreateTopic(" + offTopic + ") failed: " + e.Message);
            }

            long routeDeadline = NowMs() + 20000;
            while (queues.Count == 0 && NowMs() < routeDeadline)
            {
                try
                {
                    queues = prep.FetchPublishMessageQueues(offTopic);
                }
                catch
                {
                    queues = new List<MessageQueue>();
                }

                if (queues.Count == 0) Thread.Sleep(500);
            }

            Check("S8 prepare topic (1 queue)", queues.Count == 1, "queues=" + queues.Count);
            if (queues.Count == 1)
            {
                for (int from = 0; from < NBig; from += 300)
                {
                    var chunk = new List<Message>();
                    for (int i = from; i < Math.Min(NBig, from + 300); ++i)
                    {
                        chunk.Add(new Message(offTopic, Encoding.UTF8.GetBytes(
                            "off-" + i.ToString("D4", CultureInfo.InvariantCulture))));
                    }

                    try
                    {
                        if (prep.SendBatch(chunk).SendStatus == SendStatus.SendOk)
                        {
                            landed += chunk.Count;
                        }
                    }
                    catch (Exception e)
                    {
                        Console.WriteLine("   SendBatch failed: " + e.Message);
                    }
                }
            }

            prep.Shutdown();
        }

        Check("S8 produced " + NBig + " messages OK", landed == NBig, "landed=" + landed);

        // Independent probe reading broker offsets (own group, own connection; never touches any memory of the instances under test)
        var probe = new DefaultMQPullConsumer(offGroup);
        probe.SetNamesrvAddr(namesrv);
        probe.Start();

        MessageQueue q0 = queues.Count > 0 ? queues[0] : new MessageQueue(offTopic, "broker-a", 0);
        var o = new DefaultLitePullConsumer(offGroup);
        o.SetNamesrvAddr(namesrv);
        o.SetInstanceName("liteoff");
        o.SetPollTimeoutMillis(1000);
        o.SetPullBatchSize(32);
        o.SetAutoCommit(false);   // commit timing is fully controlled by the scenarios
        o.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        o.Assign(new[] { q0 });
        o.Start();

        // S8a pull only, no delivery: the pull cursor runs to 1200 while the consumed cursor must not move at all
        long pullDeadline = NowMs() + 30000;
        while (o.PullCursorOf(q0) < NBig && NowMs() < pullDeadline) Thread.Sleep(200);
        Check("S8a background pulled all " + NBig + " into the local buffer (pull cursor=" + o.PullCursorOf(q0) + ")",
            o.PullCursorOf(q0) == NBig, "pullCursor=" + o.PullCursorOf(q0));
        Check("S8a nothing delivered => consumed cursor stays at -1", o.ConsumeCursorOf(q0) == -1,
            "consumeCursor=" + o.ConsumeCursorOf(q0));
        // This is the core of #68: the old Commit() iterated the pull cursor and would have committed 1200 here;
        // if the caller crashed before that, 1200 messages were never consumed yet would never be delivered again.
        o.Commit();
        long afterEmptyCommit = BrokerOffset(probe, q0);
        Check("S8a nothing delivered => broker offset not pushed (old implementation would commit " + NBig + " here)",
            afterEmptyCommit != NBig, "brokerOffset=" + afterEmptyCommit);

        // S8b poll single-shot cap 1024 < 1200 in buffer => the 176-message tail does not count as consumed
        var all = new List<MessageExt>();
        List<MessageExt> first = o.Poll(3000);
        all.AddRange(first);
        Check("S8b one poll delivered 1024 messages (single-delivery cap)", first.Count == 1024,
            "got=" + first.Count);
        Check("S8b consumed cursor = the delivered slot", o.ConsumeCursorOf(q0) == 1024,
            "consumeCursor=" + o.ConsumeCursorOf(q0));
        Check("S8b buffer still holds " + (NBig - 1024) + " undelivered messages",
            o.PullCursorOf(q0) == NBig && o.ConsumeCursorOf(q0) < o.PullCursorOf(q0),
            "pull=" + o.PullCursorOf(q0));
        o.Commit();
        long brokerOffset = BrokerOffset(probe, q0);
        Check("S8b committed to the broker is exactly 1024 (not " + NBig + ")", brokerOffset == 1024,
            "brokerOffset=" + brokerOffset);

        // S8c specify an earlier offset: only the commit landing point changes; both cursors must not move
        long pullBefore = o.PullCursorOf(q0);
        long consumeBefore = o.ConsumeCursorOf(q0);
        o.Commit(new Dictionary<MessageQueue, long> { [q0] = 5 }, persist: true);
        long brokerRewound = BrokerOffset(probe, q0);
        Check("S8c commit(map) moved the broker offset to the caller-specified 5", brokerRewound == 5,
            "brokerOffset=" + brokerRewound);
        Check("S8c committing does not change the pull cursor", o.PullCursorOf(q0) == pullBefore,
            "pullCursor=" + o.PullCursorOf(q0));
        Check("S8c committing does not change the consumed cursor", o.ConsumeCursorOf(q0) == consumeBefore,
            "consumeCursor=" + o.ConsumeCursorOf(q0));
        // After rewinding to 5, the 176-message tail is still delivered (the buffer is independent of the broker offset)
        List<MessageExt> tail = o.Poll(3000);
        all.AddRange(tail);
        Check("S8c after rewinding to 5 the remaining " + (NBig - 1024) + " messages are delivered as usual",
            tail.Count == NBig - 1024, "got=" + tail.Count);
        Check("S8 received all " + NBig + " messages with none duplicated or missing",
            BodiesOf(all).Count == NBig, "distinct=" + BodiesOf(all).Count);

        // S8d not a single byte may go out: visible to Committed(), invisible to the broker
        o.Commit(new Dictionary<MessageQueue, long> { [q0] = 777 }, persist: false);
        Check("S8d persist=false: Committed() reads 777 from the in-memory table", o.Committed(q0) == 777,
            "committed=" + o.Committed(q0));
        long brokerStill = BrokerOffset(probe, q0);
        Check("S8d persist=false: broker side still has the previous round's 5", brokerStill == 5,
            "brokerOffset=" + brokerStill);

        // S8e a new instance (same group) resumes from the broker slot: the in-memory table does not cross instances
        var o2 = new DefaultLitePullConsumer(offGroup);
        o2.SetNamesrvAddr(namesrv);
        o2.SetInstanceName("liteoff2");
        o2.SetPollTimeoutMillis(1000);
        o2.SetPullBatchSize(32);
        o2.SetAutoCommit(false);
        o2.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        o2.Assign(new[] { q0 });
        o2.Start();
        long start2 = o2.PullCursorOf(q0);
        // The lower bound only requires "did not consume the in-memory 777 of the other instance": the starting pull cursor gets pushed forward by background pulling,
        // being stuck at exactly 5 would be timing-dependent, so the lower bound is 5 and the upper bound only requires not passing 777.
        Check("S8e new instance starts from the broker's 5 (not the other instance's in-memory 777)",
            start2 >= 5 && start2 < 777, "pullCursor=" + start2);
        o2.Shutdown();

        // S8f seek rewrites both cursors at once: the replayed segment must not be skipped by the old offset
        // Pause this queue first: background pulling would push the pull cursor past 60; without pausing, this assertion becomes timing-dependent.
        o.Pause(new[] { q0 });
        Thread.Sleep(300);
        o.Seek(q0, 60);
        Check("S8f seek changed the pull cursor", o.PullCursorOf(q0) == 60, "pullCursor=" + o.PullCursorOf(q0));
        Check("S8f seek also changed the consumed cursor", o.ConsumeCursorOf(q0) == 60,
            "consumeCursor=" + o.ConsumeCursorOf(q0));
        o.Commit();
        long brokerSeeked = BrokerOffset(probe, q0);
        Check("S8f after seek, commit lands at 60", brokerSeeked == 60, "brokerOffset=" + brokerSeeked);
        o.Resume(new[] { q0 });

        if (foreignPool.Count < 2)
        {
            Check("S8g requires the main topic to have at least 2 queues", false, "queues=" + foreignPool.Count);
            o.Shutdown();
            probe.Shutdown();
            return;
        }

        MessageQueue foreign = foreignPool[1];   // a queue of another topic, untouched by offGroup
        o.Assign(new[] { q0, foreign });         // only adds one more cursor; does not affect the reconciliation above
        o.Commit(new Dictionary<MessageQueue, long> { [q0] = 300 }, persist: false);
        Check("S8g un-persisted in-memory value is visible first", o.PendingCommitOf(q0) == 300,
            "pending=" + o.PendingCommitOf(q0));
        o.Commit(new List<MessageQueue>(), persist: true);
        Check("S8g empty set neither clears the table nor sends anything", o.PendingCommitOf(q0) == 300,
            "pending=" + o.PendingCommitOf(q0));
        long brokerStill2 = BrokerOffset(probe, q0);
        Check("S8g empty set left the broker untouched", brokerStill2 == 60, "brokerOffset=" + brokerStill2);
        // Naming a foreign queue: it has no consumption record (the -1 guard blocks the table write),
        // but persistAll's table scan clears the un-persisted q0 value - that is the real price of "committing a subset of queues".
        o.Commit(new[] { foreign }, persist: true);
        Check("S8g named commit clears the in-memory value of queues not named (prune of unused mqs)",
            o.PendingCommitOf(q0) == -1, "pending=" + o.PendingCommitOf(q0));
        Check("S8g after clearing, reads come back from the broker slot (and are back-filled into the table)",
            o.Committed(q0) == 60 && o.PendingCommitOf(q0) == 60,
            "committed=" + o.Committed(q0) + " pending=" + o.PendingCommitOf(q0));
        long brokerAfterPrune = BrokerOffset(probe, q0);
        Check("S8g pruning only drops the in-memory value; nothing was written to the broker as 300", brokerAfterPrune == 60,
            "brokerOffset=" + brokerAfterPrune);
        // commit(Set) takes the current consumed cursor, not the in-memory slot
        o.Commit(new Dictionary<MessageQueue, long> { [q0] = 300 }, persist: false);
        o.Commit(new[] { q0 }, persist: true);
        long brokerSet = BrokerOffset(probe, q0);
        Check("S8g commit(Set) commits the consumed cursor (60), not the in-memory 300", brokerSet == 60,
            "brokerOffset=" + brokerSet);
        Check("S8g after persistAll the in-memory table returns to the consumed cursor",
            o.PendingCommitOf(q0) == 60, "pending=" + o.PendingCommitOf(q0));

        o.Shutdown();
        probe.Shutdown();
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
        string group10 = "LitePG10Net_" + Stamp;   // S7d consistent hash ring
        string group11 = "LitePG11Net_" + Stamp;   // S7e machine-room nearby
        string group12 = "LitePG12Net_" + Stamp;   // S7f machine-room misconfigured

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("LitePullConsumer live (C#): namesrv=" + namesrv + " topic=" + Topic
            + " group=" + group1);
        Console.WriteLine(new string('=', 70));

        // ---------------- create topics ----------------
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

        // ---------------- S1 subscribe mode: start the consumer before producing ----------------
        Console.WriteLine();
        Console.WriteLine("S1 subscribe mode started + waiting for rebalance to assign queues");
        var c1 = new DefaultLitePullConsumer(group1);
        c1.SetNamesrvAddr(namesrv);
        c1.SetPollTimeoutMillis(1000);
        c1.Subscribe(Topic, "*");
        c1.Start();

        List<MessageQueue> assigned = WaitAssignment(c1);
        Check("S1 rebalance assigned " + QueueNum.ToString(CultureInfo.InvariantCulture) + " queues",
            assigned.Count == QueueNum,
            "assigned=" + assigned.Count.ToString(CultureInfo.InvariantCulture));
        if (assigned.Count == 0)
        {
            Console.WriteLine("!! no queue assigned; skipping the rest");
            c1.Shutdown();
            Console.WriteLine();
            Console.WriteLine("LitePullConsumer: PASS=" + _pass + " FAIL=" + _fail);
            return 1;
        }

        // ---------------- S2 produce + poll receive-all ----------------
        Console.WriteLine();
        Console.WriteLine("S2 producing " + NMsg.ToString(CultureInfo.InvariantCulture) + " messages (alternating TagA/TagB)");
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

            Check("S2 produced " + NMsg.ToString(CultureInfo.InvariantCulture) + " messages OK", sentOk == NMsg,
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
            Check("S2 subscribe+poll received all "
                + NMsg.ToString(CultureInfo.InvariantCulture) + " messages with matching content", gotSet.SetEquals(sent),
                "got=" + gotSet.Count.ToString(CultureInfo.InvariantCulture)
                + " missing=[" + Join(missing) + "] extra=[" + Join(extra) + "]");
        }

        // ---------------- S3 auto-commit offsets ----------------
        Console.WriteLine();
        Console.WriteLine("S3 auto-commit offsets");
        {
            bool allPositive = false;
            var detail = new StringBuilder();
            long spinDeadline = NowMs() + 20000;
            while (true)
            {
                c1.Poll(500);   // an idle cycle still counts as a poll
                allPositive = true;
                detail.Clear();
                foreach (MessageQueue mq in assigned)
                {
                    long v = c1.Committed(mq);
                    detail.Append(v.ToString(CultureInfo.InvariantCulture)).Append(' ');
                    if (v <= 0)
                    {
                        allPositive = false;
                    }
                }

                if (allPositive || NowMs() >= spinDeadline)
                {
                    break;
                }
            }

            Check("S3 after another auto-commit cycle the offsets persist on their own (nobody called commit)", allPositive,
                "committed=" + detail);
        }

        // ---------------- S4 assign mode: assign all queues + seek to queue head and receive all again ----------------
        Console.WriteLine();
        Console.WriteLine("S4 assign mode: assign all queues + seek to queue head and receive all again");
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

            Check("S4 assign+seek+poll received all "
                + NMsg.ToString(CultureInfo.InvariantCulture) + " messages", got2Set.SetEquals(sent),
                "got=" + got2Set.Count.ToString(CultureInfo.InvariantCulture));
            c2.Shutdown();
        }

        // ---------------- S5 subscription-level tag filtering ----------------
        Console.WriteLine();
        Console.WriteLine("S5 subscribing to TagA: receive only the 6 TagA messages");
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

            // Even indexes (lite-00/02/...) are the TagA ones
            bool onlyA = got3Set.Count == 6
                && got3Set.All(b => (b[^1] - '0') % 2 == 0);
            Check("S5 received only TagA and exactly 6 messages", onlyA,
                "got=" + got3Set.Count.ToString(CultureInfo.InvariantCulture));
            c3.Shutdown();
        }

        // ---------------- S6 CONSUME_FROM_TIMESTAMP: 14-digit local wall clock ----------------
        Console.WriteLine();
        Console.WriteLine("S6 CONSUME_FROM_TIMESTAMP: wall-clock start + timestamp-to-offset mapping");
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

            Check("S6a start=" + c4.ConsumeTimestamp + " earlier than all messages -> received all "
                + NMsg.ToString(CultureInfo.InvariantCulture) + " messages", got4Set.SetEquals(sent),
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
            long sumPast = q5.Sum(mq => c5.OffsetForTimestamp(mq, wall - 30 * 60 * 1000));
            long sumFuture = q5.Sum(mq => c5.OffsetForTimestamp(mq, wall + 10 * 60 * 1000));
            Check("S6b 30 minutes ago -> queue head of every queue", sumPast == 0, "sumOffset=" + sumPast);
            Check("S6b 10 minutes later -> past all " + NMsg.ToString(CultureInfo.InvariantCulture) + " messages",
                sumFuture == NMsg, "sumOffset=" + sumFuture);
            c5.Shutdown();
        }

        // ---------------- S7 pluggable queue assignment strategies ----------------
        Console.WriteLine();
        Console.WriteLine("S7 pluggable queue assignment strategies");
        {
            List<MessageQueue> allQueues = c1.FetchMessageQueues(Topic);
            allQueues.Sort();
            int totalQueues = allQueues.Count;

            var probe = new DefaultLitePullConsumer(group6);
            probe.SetNamesrvAddr(namesrv);
            Check("S7a default strategy name = AVG",
                probe.AllocateMessageQueueStrategy?.GetName() == "AVG",
                "name=" + (probe.AllocateMessageQueueStrategy?.GetName() ?? "<null>"));
            probe.SetAllocateMessageQueueStrategy(null);
            probe.Subscribe(Topic, "*");
            try
            {
                probe.Start();
                Check("S7a null strategy: Start() reports the protocol-exact message", false, "no exception thrown");
                probe.Shutdown();
            }
            catch (MQClientException e)
            {
                Check("S7a null strategy: Start() reports the protocol-exact message",
                    e.Message.Contains("allocateMessageQueueStrategy is null", StringComparison.Ordinal),
                    e.Message);
            }

            // S7b AVG_BY_CIRCLE: two instances of the same group interleave the split, no overlap and no gap
            var circle = new AllocateMessageQueueAveragelyByCircle();
            var ca = new DefaultLitePullConsumer(group7);
            var cb = new DefaultLitePullConsumer(group7);
            // Two instances in the same process must have different instanceNames (clientId prefix); otherwise the broker side sees only one consumer
            ca.SetInstanceName("s7ca");
            cb.SetInstanceName("s7cb");
            foreach (DefaultLitePullConsumer c in new[] { ca, cb })
            {
                c.SetNamesrvAddr(namesrv);
                c.SetPollTimeoutMillis(1000);
                c.SetAllocateMessageQueueStrategy(circle);
                c.Subscribe(Topic, "*");
            }

            Check("S7b after replacement strategy name = AVG_BY_CIRCLE",
                ca.AllocateMessageQueueStrategy?.GetName() == "AVG_BY_CIRCLE");
            ca.Start();
            cb.Start();
            (List<MessageQueue> qa, List<MessageQueue> qb) = WaitSplitAssignment(ca, cb, totalQueues);
            HashSet<string> ka = QueueKeys(qa), kb = QueueKeys(qb);
            var overlap = new HashSet<string>(ka.Intersect(kb, StringComparer.Ordinal));
            var both = new HashSet<string>(ka.Union(kb, StringComparer.Ordinal));
            Check("S7b the two instances' assignments do not overlap", overlap.Count == 0, "overlap=[" + KeySetText(overlap) + "]");
            Check("S7b union covers all " + totalQueues.ToString(CultureInfo.InvariantCulture) + " queues",
                both.SetEquals(QueueKeys(allQueues)),
                "a=" + ka.Count.ToString(CultureInfo.InvariantCulture)
                + " b=" + kb.Count.ToString(CultureInfo.InvariantCulture));
            // Signature of ring assignment: what you get is the interleaved queues of "index modulo", not contiguous segments
            // (4 queues / 2 instances -> 2 each with index step 2; AVG would give two contiguous segments).
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

            Check("S7b assignment shape is interleaved (step 2), not AVG's contiguous segments",
                totalQueues != 4 || circleShape,
                "posA=[" + Join(posA.Select(x => x.ToString(CultureInfo.InvariantCulture))) + "]");
            ca.Shutdown();
            cb.Shutdown();

            // S7c CONFIG: only the configured half of queues is assigned; polled messages can only come from those queues
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
            Check("S7c CONFIG assigns only the configured queues (ignoring mqAll/cidAll)",
                QueueKeys(a6).SetEquals(QueueKeys(halfA)) && QueueKeys(a7).SetEquals(QueueKeys(halfB)),
                "a=[" + KeySetText(QueueKeys(a6)) + "] b=[" + KeySetText(QueueKeys(a7)) + "]");
            // Two CONFIG consumers each see one half: union is exactly all 12 and intersection is empty -> the strategy really drives consumption
            List<MessageExt> g6 = DrainFor(c6, 8000);
            List<MessageExt> g7 = DrainFor(c7, 8000);
            HashSet<string> cfgKeysA = QueueKeys(halfA), cfgKeysB = QueueKeys(halfB);
            HashSet<string> b6 = BodiesOf(g6), b7 = BodiesOf(g7);
            HashSet<string> onCfgA = BodiesOf(g6, cfgKeysA), onCfgB = BodiesOf(g7, cfgKeysB);
            Check("S7c CONFIG consumer received only messages from its configured queues",
                b6.SetEquals(onCfgA) && b7.SetEquals(onCfgB),
                "a=" + b6.Count.ToString(CultureInfo.InvariantCulture)
                + " aOnCfg=" + onCfgA.Count.ToString(CultureInfo.InvariantCulture)
                + " b=" + b7.Count.ToString(CultureInfo.InvariantCulture)
                + " bOnCfg=" + onCfgB.Count.ToString(CultureInfo.InvariantCulture));
            var unionBodies = new HashSet<string>(b6.Union(b7, StringComparer.Ordinal));
            var interBodies = new HashSet<string>(b6.Intersect(b7, StringComparer.Ordinal));
            Check("S7c the two halves together cover exactly all "
                + NMsg.ToString(CultureInfo.InvariantCulture) + " messages without overlap",
                unionBodies.SetEquals(sent) && interBodies.Count == 0,
                "union=" + unionBodies.Count.ToString(CultureInfo.InvariantCulture)
                + " inter=" + interBodies.Count.ToString(CultureInfo.InvariantCulture));
            c6.Shutdown();
            c7.Shutdown();

            {
                var ch = new AllocateMessageQueueConsistentHash();
                var d1 = new DefaultLitePullConsumer(group10);
                var d2 = new DefaultLitePullConsumer(group10);
                SetupConsumer(d1, namesrv, "s7da", ch);
                SetupConsumer(d2, namesrv, "s7db", ch);
                Check("S7d after replacement strategy name = CONSISTENT_HASH",
                    d1.AllocateMessageQueueStrategy?.GetName() == "CONSISTENT_HASH",
                    d1.AllocateMessageQueueStrategy?.GetName() ?? "<null>");
                d1.Start();
                d2.Start();
                (bool converged, string detail) = WaitUntilPredictionConverged(
                    group10, allQueues, new[] { d1, d2 }, new[] { d1, d2 }, new IAllocateMessageQueueStrategy[] { ch, ch });
                Check("S7d online assignment converged to the offline prediction of the consistent hash ring", converged, detail);
                HashSet<string> k1 = QueueKeys(d1.Assignment()), k2 = QueueKeys(d2.Assignment());
                Check("S7d the two instances' assignments do not overlap", !k1.Overlaps(k2),
                    "overlap=[" + KeySetText(k1.Intersect(k2, StringComparer.Ordinal)) + "]");
                Check("S7d union covers all " + totalQueues.ToString(CultureInfo.InvariantCulture) + " queues",
                    new HashSet<string>(k1.Union(k2, StringComparer.Ordinal)).SetEquals(QueueKeys(allQueues)),
                    "a=" + k1.Count.ToString(CultureInfo.InvariantCulture)
                    + " b=" + k2.Count.ToString(CultureInfo.InvariantCulture));
                HashSet<string> joined = BodiesOf(DrainFor(d1, 8000));
                joined.UnionWith(BodiesOf(DrainFor(d2, 8000)));
                Check("S7d the two instances together received all "
                    + NMsg.ToString(CultureInfo.InvariantCulture) + " messages (the ring really drives consumption)",
                    joined.SetEquals(sent), "union=" + joined.Count.ToString(CultureInfo.InvariantCulture));
                d1.Shutdown();
                d2.Shutdown();
            }

            // S7e MACHINE_ROOM_NEARBY: the real cluster has a single room => the decorator must pass the inner strategy through unchanged.
            {
                var inner = new AllocateMessageQueueConsistentHash();
                var resolver = new OneRoom();
                var nearby = new AllocateMachineRoomNearby(inner, resolver);
                var e1 = new DefaultLitePullConsumer(group11);
                var e2 = new DefaultLitePullConsumer(group11);
                SetupConsumer(e1, namesrv, "s7ea", nearby);
                SetupConsumer(e2, namesrv, "s7eb", nearby);
                Check("S7e decorated strategy name = MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
                    e1.AllocateMessageQueueStrategy?.GetName() == "MACHINE_ROOM_NEARBY-CONSISTENT_HASH",
                    e1.AllocateMessageQueueStrategy?.GetName() ?? "<null>");
                e1.Start();
                e2.Start();
                (bool converged, string detail) = WaitUntilPredictionConverged(
                    group11, allQueues, new[] { e1, e2 }, new[] { e1, e2 },
                    new IAllocateMessageQueueStrategy[] { inner, inner });
                Check("S7e NEARBY's online assignment == the inner ring's offline prediction", converged, detail);
                HashSet<string> k1 = QueueKeys(e1.Assignment()), k2 = QueueKeys(e2.Assignment());
                Check("S7e NEARBY's two instances do not overlap and leave no gap",
                    !k1.Overlaps(k2)
                    && new HashSet<string>(k1.Union(k2, StringComparer.Ordinal)).SetEquals(QueueKeys(allQueues)),
                    "a=" + k1.Count.ToString(CultureInfo.InvariantCulture)
                    + " b=" + k2.Count.ToString(CultureInfo.InvariantCulture));
                // The resolver really was called by rebalance, and saw the real brokerName + two real clientIds.
                List<string> brokerCalls = resolver.BrokerCalls();
                var seenBrokers = new HashSet<string>(brokerCalls, StringComparer.Ordinal);
                var realBrokers = new HashSet<string>(allQueues.Select(mq => mq.BrokerName), StringComparer.Ordinal);
                Check("S7e resolver was asked per queue for machine rooms ("
                    + brokerCalls.Count.ToString(CultureInfo.InvariantCulture) + " times)",
                    brokerCalls.Count > 0 && seenBrokers.SetEquals(realBrokers),
                    "brokers=[" + KeySetText(seenBrokers) + "]");
                HashSet<string> calls = new(resolver.ConsumerCalls(), StringComparer.Ordinal);
                Check("S7e resolver was asked about both real clientIds",
                    calls.Contains(e1.ClientId) && calls.Contains(e2.ClientId),
                    "calls=[" + KeySetText(calls) + "]");
                e1.Shutdown();
                e2.Shutdown();
            }

            {
                var room = new AllocateMessageQueueByMachineRoom(new[] { OneRoom.Room });
                Check("S7f strategy name = MACHINE_ROOM and the whitelist reads back",
                    room.GetName() == "MACHINE_ROOM" && room.GetConsumeridcs().Contains(OneRoom.Room),
                    "idcs=[" + KeySetText(room.GetConsumeridcs()) + "]");
                var avg = new AllocateMessageQueueAveragely();
                var f1 = new DefaultLitePullConsumer(group12);
                var f2 = new DefaultLitePullConsumer(group12);
                SetupConsumer(f1, namesrv, "s7fa", room);
                SetupConsumer(f2, namesrv, "s7fb", avg);
                f1.Start();
                f2.Start();
                List<MessageQueue> ctrl = WaitAssignment(f2);
                Check("S7f same-group control group (AVG) assigns queues normally", ctrl.Count > 0,
                    "ctrl=[" + KeySetText(QueueKeys(ctrl)) + "]");
                Check("S7f machine room does not match the real brokerName -> assigns nothing (no error, no mistaken consumption)",
                    f1.Assignment().Count == 0, "assignment=[" + KeySetText(QueueKeys(f1.Assignment())) + "]");
                (bool converged, string detail) = WaitUntilPredictionConverged(
                    group12, allQueues, new[] { f1, f2 }, new[] { f1, f2 },
                    new IAllocateMessageQueueStrategy[] { room, avg });
                Check("S7f both sides' online assignments converged to their own strategy's offline prediction", converged, detail);
                List<MessageExt> starved = DrainFor(f1, 5000);
                Check("S7f the starved side polls no messages and throws no error", starved.Count == 0,
                    "got=" + starved.Count.ToString(CultureInfo.InvariantCulture));
                f1.Shutdown();
                f2.Shutdown();
            }
        }

        c1.Shutdown();

        // ---------------- S8 three offset tables ----------------
        // Unit tests can lock the table shape but not "committing to the wrong slot silently loses messages in production": so every number is
        // counted on the broker. Use a new 1-queue topic so all 1200 messages land on one contiguous offset segment.
        Console.WriteLine();
        Console.WriteLine("S8 three offset tables: pull cursor / consumed cursor / commit landing");
        ThreeOffsetTable(namesrv, assigned);

        Console.WriteLine();
        Console.WriteLine("LitePullConsumer: PASS=" + _pass + " FAIL=" + _fail);
        return _fail == 0 ? 0 : 1;
    }
}
