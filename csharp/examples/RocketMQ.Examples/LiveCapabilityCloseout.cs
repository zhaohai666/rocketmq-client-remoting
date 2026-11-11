// Live verification of the 2026-11-11 capability closeout on the .NET end (Java 5.x parity).
// Usage: rmq capability-closeout [namesrv]
//
// The three capabilities added in that round are only worth a real broker if they are either
// wire-visible or change **which messages a client sees**, so each one is asserted against a
// real 5.5.1 cluster here, never against a fake:
//
// L1 ConsumeFromWhere's three "deprecated" values.
//    Java folds CONSUME_FROM_LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST / MIN_OFFSET / MAX_OFFSET
//    into the CONSUME_FROM_LAST_OFFSET branch of
//    RebalancePushImpl#computePullFromWhereWithException (lines 171-173). That fold is pinned by
//    the offline unit suite, which reads the resolved offset from a fake store -- it is **not**
//    pinned here, because live delivery counts cannot see which branch ran:
//      * on a fresh topic the broker answers QUERY_CONSUMER_OFFSET for a never-committed group
//        with SUCCESS offset=0 on its V3_0_6 path (minOffset<=0 and offset 0 still in memory)
//        and QUERY_NOT_FOUND otherwise;
//      * which of the two it answers flips **between legs**. The Java reference client itself
//        produced FIRST=8 LAST=0 MIN=8 MAX=8 LAST_OR_MIN=0 on this same kind of backlog.
//    An equality assertion ("deprecated == this end's LAST_OFFSET baseline") therefore flakes --
//    it would have failed on Java itself. So the live legs assert only what is deterministic:
//    FIRST_OFFSET (positive control) must deliver the whole backlog, and every LAST-family value
//    (LAST_OFFSET plus the three deprecated ones) must be accepted (the consumer starts, gets a
//    real queue assignment, delivers a sane 0..N count and raises nothing). The absolute counts
//    are printed as the parity record.
//    The "three deprecated values fold into the LAST_OFFSET branch" claim is pinned by the
//    offline unit suite (it reads the resolved offset with a fake store), not by live counts.
//
// L2 LitePull pull-from-which-node pin (connectBrokerByUser / defaultBrokerId).
//    Java PullAPIWrapper#recalculatePullFromWhichNode pins the id on the first line and ignores
//    the broker's suggestion table -- the only way to read from a slave. This port keeps no
//    suggestion table on the lite path, so LiteBrokerIdFor() is exactly the value a pull round
//    puts on the request; LiteBrokerIdForTest() exposes it, which makes the assertion a
//    request-level one rather than a behavioural guess.
//        pin off          -> no broker id on the request, and the backlog is delivered
//        pin 0 (master)   -> every request carries brokerId=0, backlog delivered
//        pin 1 (no slave) -> every request carries brokerId=1, and the backlog is **still**
//                            delivered: Java resolves the address with
//                            FindBrokerAddressInSubscribe(..., onlyThisBroker=false), which
//                            falls back to the first broker in the table when the requested id
//                            is absent. That fallback is parity, not a hole -- so the assertion
//                            is on the id that travels, not on the outcome.
//    This single-broker cluster has no slave, so "reads the slave" cannot be shown here.
//
// L3 adminExtGroup.
//    A pure identity bit with no wire effect (this port's admin sends no synthetic consumer
//    heartbeat), so the live leg only proves admin still boots and serves an RPC with a custom
//    group, plus the default/override values themselves.
//
// Every leg gets a **fresh group**: a group that has already committed offsets reads them back
// whatever consumeFromWhere says, which is exactly the false green this scenario must avoid.
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveCapabilityCloseout
{
    private const int NMsg = 8;
    private const string ExpectedAdminExtGroup = "admin_ext_group";

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

    private static string N(long v) => v.ToString(CultureInfo.InvariantCulture);

    private static string Join(IEnumerable<string> items) => "[" + string.Join(", ", items) + "]";

    /// <summary>Real-machine delivery is affected by long polling / flow control / load on the
    /// same machine, so a fixed sleep would turn "the implementation is fine" into a false
    /// failure.</summary>
    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 200)
    {
        long deadline = NowMs() + timeoutMs;
        while (NowMs() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(intervalMs);
        }

        return pred();
    }

    /// <summary>Records every body a push consumer delivers.</summary>
    private sealed class CollectingListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<string> _bodies = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                foreach (MessageExt m in msgs) _bodies.Add(Encoding.UTF8.GetString(m.Body));
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public int Count()
        {
            lock (_lk) return _bodies.Count;
        }

        public List<string> Snapshot()
        {
            lock (_lk) return new List<string>(_bodies);
        }
    }

    /// <summary>
    /// One push leg: a fresh group started at <paramref name="where"/>, counting arrivals for
    /// <paramref name="windowMs"/>.
    /// <para>The leg waits for a real queue assignment first: without one the consumer holds no
    /// queue, nothing can arrive, and an assignment bug would look like a consumeFromWhere
    /// bug.</para>
    /// </summary>
    private static (int Got, bool Assigned, List<string> QueueKeys) PushLeg(
        string namesrv, string topic, string group, string where, int windowMs)
    {
        var sink = new CollectingListener();
        var c = new DefaultMQPushConsumer(group)
        {
            InstanceName = "cap-" + group,
            ConsumeFromWhere = where,
        };
        c.SetNamesrvAddr(namesrv);
        c.Subscribe(topic, "*");
        c.SetConsumeThreadNums(1);
        c.SetMessageListener(sink);
        c.Start();
        try
        {
            bool assigned = WaitUntil(() => c.AssignedQueueKeys().Count > 0, 25000);
            List<string> keys = c.AssignedQueueKeys();
            long deadline = NowMs() + windowMs;
            while (NowMs() < deadline) Thread.Sleep(200);
            return (sink.Count(), assigned, keys);
        }
        finally
        {
            c.Shutdown();
        }
    }

    /// <summary>One LitePull subscribe-mode leg with the given pin.
    /// <para><c>Resolved</c> is the broker id a pull round would put on the request: with the pin
    /// off that is null (the address comes from the route), with the pin on it is
    /// <paramref name="brokerId"/>.</para></summary>
    private static (int Got, long? Resolved, bool Pin) LiteLeg(
        string namesrv, string topic, string group, bool pin, long brokerId, int want,
        int windowMs, MessageQueue probeQueue)
    {
        var c = new DefaultLitePullConsumer(group);
        c.SetNamesrvAddr(namesrv);
        c.SetInstanceName("cap-lite-" + group);
        c.SetPollTimeoutMillis(1000);
        c.SetPullBatchSize(32);
        c.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        c.SetConnectBrokerByUser(pin);
        c.SetDefaultBrokerId(brokerId);
        c.Subscribe(topic, "*");
        c.Start();
        try
        {
            // Read the switch and the id the pull path would put on the request from the same
            // function the pull round calls, not by re-deriving the expectation from the fields.
            bool pinOn = c.IsConnectBrokerByUser;
            long? resolved = c.LiteBrokerIdForTest(probeQueue);
            int got = 0;
            long deadline = NowMs() + windowMs;
            while (NowMs() < deadline && got < want)
            {
                got += c.Poll(1000).Count;
            }

            return (got, resolved, pinOn);
        }
        finally
        {
            c.Shutdown();
        }
    }

    /// <summary>One LAST-family leg. PASS iff the consumer started, got a real queue assignment
    /// and delivered a sane 0..N count with no exception -- see the header for why the count
    /// itself is not asserted against a baseline.</summary>
    private static (bool Ok, int Count, string Detail) LastFamilyLeg(
        string namesrv, string topic, string group, string where, int windowMs)
    {
        try
        {
            (int n, bool assigned, _) = PushLeg(namesrv, topic, group, where, windowMs);
            return (assigned && n >= 0 && n <= NMsg, n, "received=" + N(n) + " assigned="
                + (assigned ? "true" : "false"));
        }
        catch (Exception e)
        {
            // An exception on one leg must not hide the other three.
            return (false, -1, "error=" + e.GetType().Name + ": " + e.Message);
        }
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        string stamp = NowMs().ToString(CultureInfo.InvariantCulture);
        string prefix = "CapLive_" + stamp;
        string t1 = prefix + "_FromWhere";
        string t2 = prefix + "_Pin";
        var bodies = new List<string>();
        for (int i = 0; i < NMsg; ++i) bodies.Add("cap-" + i.ToString("D2", CultureInfo.InvariantCulture));

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("Capability closeout live (C#): namesrv=" + namesrv + " prefix=" + prefix);
        Console.WriteLine(new string('=', 70));

        var admin = new DefaultMQAdminExt("cap-closeout-admin-" + stamp);
        admin.SetNamesrvAddr(namesrv);
        admin.SetTimeoutMillis(10000);
        var producer = new DefaultMQProducer(prefix + "_pg")
        {
            NamesrvAddr = namesrv,
            InstanceName = "cap-closeout-prod-" + stamp,
        };
        string brokerAddr = "";
        bool setupOk = false;
        try
        {
            admin.Start();
            try
            {
                List<string> addrs = admin.FetchBrokerClusterInfo().GetBrokerAddrs();
                brokerAddr = addrs.Count > 0 ? addrs[0] : "";
            }
            catch (Exception e)
            {
                brokerAddr = "";
                Console.WriteLine("  [diag] fetchBrokerClusterInfo failed: " + e.Message);
            }

            Check("the cluster liveness probe", brokerAddr.Length > 0, "broker=" + brokerAddr);
            if (brokerAddr.Length == 0) return Report();

            // Create both topics up front: a consumer gets no route for an uncreated topic
            // (TBW102 is a producer-side fallback only).
            admin.CreateTopic(MixAll.DefaultTopic, t1, 1);
            admin.CreateTopic(MixAll.DefaultTopic, t2, 1);

            // A createTopic only reaches the name server on the broker's own registration round;
            // a send before that lands through the TBW102 fallback and a consumer sees no route.
            bool RouteReady()
            {
                try
                {
                    return admin.ExamineTopicRoute(t1).GetAllSubscribeMessageQueue(t1).Count >= 1
                        && admin.ExamineTopicRoute(t2).GetAllSubscribeMessageQueue(t2).Count >= 1;
                }
                catch (Exception)
                {
                    return false;
                }
            }

            bool route = WaitUntil(RouteReady, 30000);
            Check("the topic route is visible before anything is sent", route);
            if (!route) return Report();

            producer.Start();

            int seeded1 = Seed(producer, t1, bodies);
            Check("seeded topic=" + t1, seeded1 == NMsg, "sent=" + N(seeded1));
            int seeded2 = Seed(producer, t2, bodies);
            Check("seeded topic=" + t2, seeded2 == NMsg, "sent=" + N(seeded2));
            if (seeded1 != NMsg || seeded2 != NMsg) return Report();

            setupOk = true;
        }
        finally
        {
            if (!setupOk)
            {
                try
                {
                    producer.Shutdown();
                }
                catch (Exception)
                {
                    // a cleanup failure does not mask the main assertion
                }

                try
                {
                    admin.Shutdown();
                }
                catch (Exception)
                {
                }
            }
        }

        try
        {
            // ------------------------------------------------------------ L1
            Console.WriteLine();
            Console.WriteLine("L1 ConsumeFromWhere: the three deprecated values fold into LAST_OFFSET");
            Console.WriteLine("  sent " + N(NMsg) + " messages, no consumer has ever joined this topic");

            const int window = 12000;
            (int got, bool assigned, List<string> keys) = PushLeg(
                namesrv, t1, prefix + "_gFirst", ConsumeFromWhere.ConsumeFromFirstOffset, window);
            Check("L1-the positive-control consumer really got a queue assigned", assigned,
                "keys=" + Join(keys));
            Check("L1-positive control: FIRST_OFFSET delivers the whole backlog", got == NMsg,
                "received=" + N(got) + " expect=" + N(NMsg));

            // Why only acceptance is asserted for the LAST family and not "== baseline": see the
            // header. The broker's V3_0_6 answer for a never-committed group flips between legs,
            // so the counts are information, not an oracle. LAST_OFFSET is one of the four legs
            // for the same reason -- it is not a reference the others must match.
            var parity = new List<string>();
            foreach ((string label, string where) in new[]
                     {
                         ("LAST_OFFSET", ConsumeFromWhere.ConsumeFromLastOffset),
                         ("MIN_OFFSET", ConsumeFromWhere.ConsumeFromMinOffset),
                         ("MAX_OFFSET", ConsumeFromWhere.ConsumeFromMaxOffset),
                         ("LAST_OFFSET_AND_FROM_MIN_WHEN_BOOT_FIRST",
                             ConsumeFromWhere.ConsumeFromLastOffsetAndFromMinWhenBootFirst),
                     })
            {
                (bool ok, int n, string detail) = LastFamilyLeg(
                    namesrv, t1, prefix + "_g" + label, where, window);
                parity.Add(label + "=" + (n < 0 ? "error" : N(n)));
                if (label == "LAST_OFFSET")
                {
                    Console.WriteLine("  INFO LAST_OFFSET baseline on this end: received="
                        + (n < 0 ? "n/a" : N(n)) + " of " + N(NMsg) + " (0 = tail start, "
                        + N(NMsg) + " = whole backlog replayed)");
                }

                Check("L1-LAST-family " + label + " is accepted on the live cluster (started, a "
                    + "real queue assignment, a sane 0.." + N(NMsg) + " count, no exception)",
                    ok, detail);
            }

            Console.WriteLine("  INFO LAST-family delivery counts on this end: {"
                + string.Join(", ", parity) + "}");
            Console.WriteLine("  INFO a never-committed group's QUERY_CONSUMER_OFFSET answer is "
                + "timing-dependent on the broker's V3_0_6 path, so these counts are a parity "
                + "record, not an oracle; the fold into the LAST_OFFSET branch is pinned by the "
                + "offline unit suite, not by live delivery counts");

            // ------------------------------------------------------------ L2
            Console.WriteLine();
            Console.WriteLine("L2 LitePull pin (connectBrokerByUser / defaultBrokerId)");
            Console.WriteLine("  sent " + N(NMsg) + " messages to " + t2);
            MessageQueue probe = admin.ExamineTopicRoute(t2).GetAllSubscribeMessageQueue(t2)[0];

            (int nOff, long? idOff, bool pinOff) = LiteLeg(
                namesrv, t2, prefix + "_pinOff", false, 0, NMsg, 20000, probe);
            Check("L2-pin off: the request carries no broker id (route resolution as before)",
                pinOff == false && idOff is null,
                "pin=" + (pinOff ? "true" : "false") + " resolved=" + (idOff is null ? "null" : N(idOff.Value)));
            Check("L2-pin off: delivers the backlog", nOff == NMsg,
                "got=" + N(nOff) + " expect=" + N(NMsg));

            (int nMaster, long? idMaster, bool pinMaster) = LiteLeg(
                namesrv, t2, prefix + "_pinMaster", true, 0, NMsg, 20000, probe);
            Check("L2-pin to the master (0): every pull request carries brokerId=0",
                pinMaster && idMaster == 0,
                "pin=" + (pinMaster ? "true" : "false") + " resolved="
                + (idMaster is null ? "null" : N(idMaster.Value)));
            Check("L2-pin to the master (0): delivers the backlog", nMaster == NMsg,
                "got=" + N(nMaster) + " expect=" + N(NMsg));

            (int nSlave, long? idSlave, bool pinSlave) = LiteLeg(
                namesrv, t2, prefix + "_pinSlave", true, 1, NMsg, 12000, probe);
            Check("L2-pin to a non-existent slave (1): every pull request carries brokerId=1",
                pinSlave && idSlave == 1,
                "pin=" + (pinSlave ? "true" : "false") + " resolved="
                + (idSlave is null ? "null" : N(idSlave.Value)));
            Check("L2-pin to a non-existent slave (1): FindBrokerAddressInSubscribe(..., "
                + "onlyThisBroker=false) falls back to the table's first broker, so delivery "
                + "still happens (Java parity, not a hole)", nSlave == NMsg,
                "got=" + N(nSlave) + " expect=" + N(NMsg));

            // ------------------------------------------------------------ L3
            Console.WriteLine();
            Console.WriteLine("L3 adminExtGroup (identity only, no wire effect)");
            Check("L3-DefaultAdminExtGroup matches the Java constant",
                DefaultMQAdminExt.DefaultAdminExtGroup == ExpectedAdminExtGroup,
                "value=" + DefaultMQAdminExt.DefaultAdminExtGroup);

            // A separate instance with its own instanceName: two admins sharing one clientId
            // would share the underlying MQClientInstance, and the leg must exercise a fresh
            // identity rather than the one already used for topic setup.
            var admin3 = new DefaultMQAdminExt("cap-closeout-l3-" + stamp);
            admin3.SetNamesrvAddr(namesrv);
            Check("L3-default adminExtGroup is '" + ExpectedAdminExtGroup + "'",
                admin3.GetAdminExtGroup() == ExpectedAdminExtGroup,
                "value=" + admin3.GetAdminExtGroup());
            string customGroup = "cap_closeout_admin_" + prefix;
            admin3.SetAdminExtGroup(customGroup);
            Check("L3-override takes", admin3.GetAdminExtGroup() == customGroup,
                "value=" + admin3.GetAdminExtGroup());
            try
            {
                admin3.Start();
                TopicList topics = admin3.FetchAllTopicList();
                Check("L3-a custom-group admin still boots and serves an RPC on the live cluster",
                    topics.Topics.Contains(t2), "topics=" + N(topics.Topics.Count));
            }
            finally
            {
                admin3.Shutdown();
            }
        }
        finally
        {
            try
            {
                producer.Shutdown();
            }
            catch (Exception)
            {
            }

            try
            {
                admin.DeleteTopic(t1);
                admin.DeleteTopic(t2);
            }
            catch (Exception e)
            {
                Console.WriteLine("  [WARN] deleteTopic failed: " + e.Message);
            }

            try
            {
                admin.Shutdown();
            }
            catch (Exception)
            {
            }
        }

        return Report();
    }

    private static int Seed(DefaultMQProducer producer, string topic, List<string> bodies)
    {
        int ok = 0;
        foreach (string b in bodies)
        {
            try
            {
                if (producer.Send(new Message(topic, Encoding.UTF8.GetBytes(b))).SendStatus
                    == SendStatus.SendOk)
                {
                    ++ok;
                }
            }
            catch (Exception e)
            {
                Console.WriteLine("   send to " + topic + " failed: " + e.Message);
            }
        }

        return ok;
    }

    private static int Report()
    {
        Console.WriteLine();
        Console.WriteLine("CapabilityCloseout live: PASS=" + N(_pass) + " FAIL=" + N(_fail));
        return _fail == 0 ? 0 : 1;
    }
}
