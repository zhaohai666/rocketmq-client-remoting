// Prerequisite: namesrv + master + slave are all running (following the local cluster
// runbook); the script **only stops the master once** (scripts/rmq_test_broker.sh only
// recognises the master's java process), and the cleanup unconditionally brings it back up.
//
//
// The offline unit tests (tests/RouteHeartbeatTests.cs) pin the criteria; the real machine
// pins the result of those criteria acting on a real route shape -- where broker-a really
// only has {1: slave} left in the name server.
//
//
// Usage: rmq publish-route-master [namesrv] [master] [slave]
using System.Diagnostics;
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LivePublishRouteMaster
{
    private const int Queues = 4;
    private const string BrokerName = "broker-a";

    /// <summary>The upper bound for a local failure: when the publish info is empty there is
    /// no broker wire call at all, so 500ms already leaves an order of magnitude of slack.</summary>
    private const double LocalBudgetMs = 500.0;

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

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string ScriptPath()
    {
        string root = Environment.GetEnvironmentVariable("RMQ_REPO_ROOT") ?? "..";
        return Path.Combine(root, "scripts", "rmq_test_broker.sh");
    }

    /// <summary>
   /// Runs scripts/rmq_test_broker.sh (**only touches the master**; the live cases of all
   /// four languages share it).
   /// The output goes to a **file** rather than a pipe: start turns the broker into a
    /// resident process, and whoever inherits its write end never sees EOF.
    /// </summary>
    private static (bool Ok, string Out) BrokerCtl(string script, string action)
    {
        string logPath = "/tmp/rmq_pr_master_cs_broker_ctl." + action + ".log";
        try
        {
            var psi = new ProcessStartInfo("/bin/sh",
                "-c \"sh '" + script + "' " + action + " > '" + logPath + "' 2>&1 < /dev/null\"")
            {
                UseShellExecute = false,
            };
            using Process? proc = Process.Start(psi);
            proc?.WaitForExit();
            string text = File.Exists(logPath) ? File.ReadAllText(logPath).Trim() : string.Empty;
            bool ok = proc is not null && proc.ExitCode == 0;
            return (ok, text.Length > 0 ? text : "exit=" + (proc?.ExitCode ?? -1));
        }
        catch (Exception ex)
        {
            return (false, "run " + action + " failed: " + ex.Message);
        }
    }

   /// <summary>Real-machine delivery/replication is affected by the broker's long polling and
    /// by load on the same machine, so a fixed sleep turns "the implementation is fine" into a
    /// false failure.</summary>
    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 500)
    {
        var sw = Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < timeoutMs)
        {
            if (pred())
            {
                return true;
            }

            Thread.Sleep(intervalMs);
        }

        return pred();
    }

    private static string FmtAddrs(SortedDictionary<long, string> addrs) =>
        "{" + string.Join(", ", addrs.Select(kv => kv.Key + ": " + kv.Value)) + "}";

    private static string FmtQueues(List<MessageQueue> queues) =>
        "[" + string.Join(", ", queues.Select(q => q.BrokerName + ":" + q.QueueId)) + "]";

    private static string FmtOffsets(List<(int QueueId, long Offset)> rows) =>
        "[" + string.Join(", ", rows.Select(r => "q" + r.QueueId + "=" + r.Offset)) + "]";

   /// <summary>Only collects the bodies: what this round cares about is "can the 4 messages
   /// on the slave all be received", not offsets or order.</summary>
    private sealed class Sink
    {
        private readonly object _lock = new();
        private readonly List<string> _bodies = new();

        public void Add(string body)
        {
            lock (_lock)
            {
                _bodies.Add(body);
            }
        }

        public List<string> Bodies()
        {
            lock (_lock)
            {
                return new List<string>(_bodies);
            }
        }
    }

    private sealed class BodyListener : IMessageListenerConcurrently
    {
        private readonly Sink _sink;

        public BodyListener(Sink sink) => _sink = sink;

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
        {
            foreach (MessageExt m in msgs)
            {
                _sink.Add(Encoding.UTF8.GetString(m.Body));
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    private sealed class Fixture
    {
        public string Namesrv = string.Empty;
        public string Master = string.Empty;
        public string Slave = string.Empty;
        public string Stamp = string.Empty;
        public string Topic = string.Empty;
        public DefaultMQAdminExt Admin = null!;
        public DefaultMQProducer Producer = null!;
        public DefaultMQPushConsumer? Consumer;

        public void Start(string namesrv, string master, string slave)
        {
            Namesrv = namesrv;
            Master = master;
            Slave = slave;
            Stamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                .ToString(CultureInfo.InvariantCulture);
            Topic = "PrMasterCs" + Stamp;

            Admin = new DefaultMQAdminExt();
            Admin.SetNamesrvAddr(namesrv);
            Admin.Start();

            Producer = new DefaultMQProducer("GID_PrMasterCsPg_" + Stamp)
            {
                NamesrvAddr = namesrv,
                InstanceName = "pr_master_cs_p_" + Stamp,
            };
            Producer.Start();
        }

        /// <summary>broker-a's current brokerAddrs (every call forces one route refresh; the
    /// wait for stop/recovery polls this).</summary>
        public SortedDictionary<long, string> RouteAddrs()
        {
            Admin.Client().UpdateTopicRouteInfoFromNameServer(Topic, false, 5000);
            TopicRouteData? route = Admin.Client().GetTopicRouteData(Topic);
            BrokerData? bd = route?.BrokerDatas.FirstOrDefault(b => b.BrokerName == BrokerName);
            return bd is null
                ? new SortedDictionary<long, string>()
                : new SortedDictionary<long, string>(bd.BrokerAddrs);
        }

        public List<MessageQueue> SeedQueues() =>
            Admin.Client().GetTopicPublishInfo(Topic, false).MsgQueueList;

        public long MaxOffset(MessageQueue mq, string? addr = null) =>
            Admin.Client().GetMaxOffset(mq, 5000, addr);

        public DefaultMQPushConsumer BuildConsumer(string group, Sink sink)
        {
            var c = new DefaultMQPushConsumer(group);
            c.SetNamesrvAddr(Namesrv);
            c.InstanceName = "pr_master_cs_c_" + Stamp;
            c.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
            c.ConsumeMessageBatchMaxSize = 3;
            c.SetMessageListener(new BodyListener(sink));
            c.Subscribe(Topic, "*");
            return c;
        }

        /// <summary>Cleanup: first bring the master back up (start is idempotent), then
    /// delete the topic and close the clients -- no orphan config may be left behind.</summary>
        public void Cleanup(string script)
        {
            BrokerCtl(script, "start");
            foreach (string addr in new[] { Master, Slave })
            {
                try
                {
                    Admin.Client().DeleteTopicInBroker(addr, Topic, 5000);
                }
                catch (Exception e)
                {
           Console.WriteLine("    (delete topic on " + addr + " failed: " + e.Message + ")");
                }
            }

            try
            {
                Admin.Client().DeleteTopicInNamesrv(Topic, 5000);
            }
            catch (Exception e)
            {
         Console.WriteLine("    (delete topic in namesrv failed: " + e.Message + ")");
            }

            try
            {
                Consumer?.Shutdown();
            }
            catch (Exception)
            {
    // a cleanup failure must not mask the conclusion of the case
            }

            try
            {
                Producer.Shutdown();
            }
            catch (Exception)
            {
   // same
            }

            try
            {
                Admin.Shutdown();
            }
            catch (Exception)
            {
   // same
            }
        }
    }

    /// <summary>Empty publish info => GetTopicPublishInfo throws "Can not find Message Queue
    /// for topic".</summary>
    private static bool PublishInfoEmpty(MQClientInstance client, string topic, out string detail)
    {
        try
        {
            TopicPublishInfo info = client.GetTopicPublishInfo(topic, false);
    detail = "unexpectedly got " + info.MsgQueueList.Count + " queues";
            return false;
        }
        catch (MQClientException e)
        {
            detail = e.Message;
            return e.Message.Contains("Can not find Message Queue for topic", StringComparison.Ordinal);
        }
    }

    private static void Scenario(Fixture fx, string script)
    {
        string topic = fx.Topic;

        // ---------- S0 the control leg ----------
        Console.WriteLine("S0 the control leg (master up): the route {0: master, 1: slave}, " + Queues + " queues each for publish/subscribe");
        try
        {
            fx.Admin.Client().CreateTopicInRoute(topic, Queues, Queues, MixAll.ReadPermByDefault, 5000);
        }
        catch (Exception e)
        {
     Check("S0 created the topic (master)", false, e.Message);
            return;
        }

        // Creating one directly on the slave too, not betting on SlaveSynchronize's 5s period
        try
        {
            fx.Admin.CreateTopicInBroker(fx.Slave, topic, Queues, Queues, MixAll.ReadPermByDefault);
        }
        catch (Exception e)
        {
       Console.WriteLine("    (creating the topic on the slave failed: " + e.Message + ")");
        }

        SortedDictionary<long, string> addrs = new();
        bool routeUp = WaitUntil(() =>
        {
            addrs = fx.RouteAddrs();
            return addrs.ContainsKey(0) && addrs.ContainsKey(1);
        }, 30000);
 Check("S0 the route contains {0: master, 1: slave}", routeUp, "broker_addrs=" + FmtAddrs(addrs));
        if (!routeUp)
        {
            return;
        }

        string routeSlave = addrs.TryGetValue(1, out string? s1) ? s1 : string.Empty;
   Check("S0 the slave address matches the parameter", routeSlave == fx.Slave,
            "route=" + routeSlave + " argv=" + fx.Slave);

        List<MessageQueue> queues;
        try
        {
            queues = fx.SeedQueues();
        }
        catch (Exception e)
        {
   Check("S0 the publish queues " + Queues + " (control)", false, e.Message);
            return;
        }

        queues = queues.OrderBy(q => q.QueueId).ToList();
   Check("S0 the publish queues " + Queues + " (control)", queues.Count == Queues,
            "queues=" + FmtQueues(queues));
        List<MessageQueue> subs = fx.Admin.Client().GetTopicSubscribeInfo(topic);
 Check("S0 the subscribe queues " + Queues + " (control)", subs.Count == Queues, "queues=" + FmtQueues(subs));
        if (queues.Count != Queues)
        {
            return;
        }

        // ---------- S1 seeding ----------
        Console.WriteLine();
        Console.WriteLine("S1 seeding " + Queues + " messages (one pinned per queue) and waiting for the slave's store to catch up");
        var seeded = new List<string>();
        for (int i = 0; i < queues.Count; i++)
        {
            string body = "pr-master-" + i.ToString(CultureInfo.InvariantCulture);
            var msg = new Message(topic, Bytes(body));
            try
            {
                SendResult r = fx.Producer.Send(msg, queues[i], 20000);
                if (r.SendStatus == SendStatus.SendOk)
                {
                    seeded.Add(body);
                }
                else
                {
       Check("S1 seeding message number " + i + " SEND_OK", false, "status=" + r.SendStatus);
                }
            }
            catch (Exception e)
            {
            Check("S1 seeding message number " + i + " SEND_OK", false, e.Message);
            }
        }

        Check("S1 all " + Queues + " seeded messages are SEND_OK", seeded.Count == Queues, "seeded=" + seeded.Count);
        if (seeded.Count != Queues)
        {
            return;
        }

        var masterMax = new List<(int QueueId, long Offset)>();
        foreach (MessageQueue mq in queues)
        {
            try
            {
                masterMax.Add((mq.QueueId, fx.MaxOffset(mq)));
            }
            catch (Exception e)
            {
      Console.WriteLine("    (the master's maxOffset query failed: " + e.Message + ")");
            }
        }

        bool caughtUp = WaitUntil(() =>
        {
            if (masterMax.Count == 0)
            {
                return false;
            }

            var slaveMax = new List<(int QueueId, long Offset)>();
            foreach (MessageQueue mq in queues)
            {
                try
                {
                    slaveMax.Add((mq.QueueId, fx.MaxOffset(mq, fx.Slave)));
                }
                catch (Exception e)
                {
  Console.WriteLine("    (getting maxOffset from the slave failed: " + e.Message + ")");
                    return false;
                }
            }

            return masterMax.All(m => slaveMax.Any(s => s.QueueId == m.QueueId && s.Offset >= m.Offset));
        }, 30000);
        Check("S1 the " + Queues + " messages have been replicated to the slave's store", caughtUp,
            "master=" + FmtOffsets(masterMax));

        // ---------- S2 stopping the master ----------
        Console.WriteLine();
        Console.WriteLine("S2 stopping the master (scripts/rmq_test_broker.sh stop) and waiting for the route to be left with only the slave");
        (bool stopped, string stopOut) = BrokerCtl(script, "stop");
        Check("S2 the master stopped gracefully", stopped, stopOut);

        SortedDictionary<long, string> downAddrs = new();
        bool masterless = WaitUntil(() =>
        {
            downAddrs = fx.RouteAddrs();
            return !downAddrs.ContainsKey(0) && downAddrs.ContainsKey(1);
        }, 60000);
   Check("S2 broker-a only has {1: slave} left in the route", masterless, "broker_addrs=" + FmtAddrs(downAddrs));
        if (!masterless)
        {
            return;
        }

   // ---------- S3 (A) the publish info group produces no queues ----------
        Console.WriteLine();
        Console.WriteLine("S3 (A) the publish info skips a broker with no master");
        bool s3 = PublishInfoEmpty(fx.Admin.Client(), topic, out string s3Detail);
        Check("S3 after the master stops the publish queues == 0 (the accessor throws \"cannot select a queue\" locally)", s3, s3Detail);

        // ---------- S4 (C) the subscribe queues ----------
        List<MessageQueue> subsDown = fx.Admin.Client().GetTopicSubscribeInfo(topic);
     Check("S4 (C) the subscribe queues are still " + Queues + " and all on " + BrokerName + " (the consume side does not look at the master)",
            subsDown.Count == Queues && subsDown.All(q => q.BrokerName == BrokerName),
            "queues=" + FmtQueues(subsDown));

        // ---------- S5 (A manifesting) the send fails fast ----------
        // Seen in two stages: while **the cache has not been refreshed** (in reality the 30s
        // periodic task has not fired yet) the sending instance still holds the pre-stop
   // route, the address resolution lands on the dead master, it fails fast and never
        // silently switches to the slave; once **the route has been refreshed to the
   // post-stop shape** (the periodic task / an explicit refresh), (A) takes effect: the
        // publish queues are empty and the send does not put a single broker wire out.
        Console.WriteLine();
        Console.WriteLine("S5 a synchronous send with no queue specified: stale cache fails fast -> after the refresh it fails fast locally");
        var sw = Stopwatch.StartNew();
        string staleText;
        bool staleFailed;
        try
        {
            SendResult r = fx.Producer.Send(new Message(topic, Bytes("must-not-send")), 20000);
            staleText = "SendResult(status=" + r.SendStatus + ")";
            staleFailed = false;
        }
        catch (Exception e)
        {
            staleText = e.GetType().Name + ": " + e.Message;
            staleFailed = true;
        }

        double staleMs = sw.Elapsed.TotalMilliseconds;
        // This leg is the opportunistic one: whether the periodic refresh has already run is
        // not decided by this script. The criterion common to both legs is "fails fast +
      // never lands on the slave address" (the stale-cache leg hits the dead master).
        Check("S5 the send fails fast and the error mentions no slave address (it never switches to the slave)",
            staleFailed && staleMs < LocalBudgetMs && !staleText.Contains(fx.Slave, StringComparison.Ordinal),
            staleMs.ToString("F0", CultureInfo.InvariantCulture) + "ms " + staleText);

  // Make the sending instance's own route cache refresh to the post-stop shape (the very
    // same code path as the 30s periodic task)
        MQClientInstance pcli;
        try
        {
            pcli = fx.Producer.Client();
        }
        catch (Exception e)
        {
            Check("S5b got the sending instance", false, e.Message);
            return;
        }

        try
        {
            pcli.UpdateTopicRouteInfoFromNameServer(topic, false, 5000);
        }
        catch (Exception e)
        {
            Console.WriteLine("    (the sending instance's route refresh failed: " + e.Message + ")");
        }

        bool s5b = PublishInfoEmpty(pcli, topic, out string s5bDetail);
      Check("S5b the sending instance's publish queues are == 0 too (this is where (A) acts)", s5b, s5bDetail);

        sw.Restart();
        string freshText;
        bool freshFailed = false;
        int freshCode = -1;
        try
        {
            SendResult r = fx.Producer.Send(new Message(topic, Bytes("must-not-send-2")), 20000);
            freshText = "SendResult(status=" + r.SendStatus + ")";
        }
        catch (MQClientException e)
        {
            freshFailed = true;
            freshCode = e.ResponseCode;
            freshText = e.Message;
        }
        catch (Exception e)
        {
            freshFailed = true;
            freshText = e.GetType().Name + ": " + e.Message;
        }

        double freshMs = sw.Elapsed.TotalMilliseconds;
        Check("S5c after the refresh: locally 10005 throws \"cannot select a queue\", no wire call (no BrokersSent)",
            freshFailed
            && freshCode == ClientErrorCode.NotFoundTopicException
            && freshText.Contains("Can not find Message Queue for topic", StringComparison.Ordinal)
            && !freshText.Contains("BrokersSent", StringComparison.Ordinal)
            && freshMs < LocalBudgetMs,
            freshMs.ToString("F0", CultureInfo.InvariantCulture) + "ms code=" + freshCode + ": " + freshText);

        Console.WriteLine();
     Console.WriteLine("S5d (B) the control: a pinned send to that queue -> locally reports the broker does not exist (no wire)");
        var mq0 = new MessageQueue(topic, BrokerName, 0);
        int pinnedCode = -1;
        bool pinnedFailed = false;
        bool pinnedIsClientError = false;
        string pinnedText;
        sw.Restart();
        try
        {
            SendResult r = fx.Producer.Send(new Message(topic, Bytes("pinned-to-slave")), mq0, 20000);
            pinnedText = "SendResult(status=" + r.SendStatus + ")";
        }
        catch (MQClientException e)
        {
    // MQBrokerException does not inherit MQClientException: getting in here proves it is
    // not a code the broker returned
            pinnedFailed = true;
            pinnedIsClientError = true;
            pinnedCode = e.ResponseCode;
            pinnedText = e.Message;
        }
        catch (Exception e)
        {
            pinnedFailed = true;
            pinnedText = e.GetType().Name + ": " + e.Message;
        }

        double pinnedMs = sw.Elapsed.TotalMilliseconds;
       Check("S5d the pinned send locally reports \"The broker[" + BrokerName + "] not exist\", with no wire"
          + " (not a SYSTEM_BUSY(2) returned by the slave)",
            pinnedFailed && pinnedIsClientError && pinnedCode == -1
            && pinnedText == "The broker[" + BrokerName + "] not exist"
            && pinnedMs < LocalBudgetMs,
            pinnedMs.ToString("F0", CultureInfo.InvariantCulture) + "ms code=" + pinnedCode
            + ": " + pinnedText);

        Console.WriteLine();
        Console.WriteLine("S5e (D) orderly locks: the whole machine is skipped inside the stop window (no route refresh, no wire)");
        string group = "GID_PrMasterCs_" + fx.Stamp;
        string clientId = "pr_master_cs_lock_" + fx.Stamp;
        var lockMqs = new List<MessageQueue> { new(topic, BrokerName, 0) };
        List<MessageQueue> locks = new();
        sw.Restart();
        try
        {
            locks = fx.Admin.Client().LockBatchMq(group, clientId, lockMqs, 3000);
        }
        catch (Exception e)
        {
  Console.WriteLine("    (LockBatchMq threw: " + e.Message + ")");
        }

        double lockMs = sw.Elapsed.TotalMilliseconds;
      Check("S5e not a single one can be locked when only the slave is left (the old rule fell back to locking on the slave)",
            locks.Count == 0 && lockMs < LocalBudgetMs,
            lockMs.ToString("F0", CultureInfo.InvariantCulture) + "ms locked=" + FmtQueues(locks));

     // The control leg: the same request naming the slave directly -- the slave **does**
  // hand out a lock for it
        JsonValue lockBody = JsonValue.MakeObject();
        lockBody.Set("consumerGroup", JsonValue.MakeString(group));
        lockBody.Set("clientId", JsonValue.MakeString(clientId));
        JsonValue mqArr = JsonValue.MakeArray();
        JsonValue mqObj = JsonValue.MakeObject();
        mqObj.Set("topic", JsonValue.MakeString(topic));
        mqObj.Set("brokerName", JsonValue.MakeString(BrokerName));
        mqObj.Set("queueId", JsonValue.MakeInt(0));
        mqArr.PushArray(mqObj);
        lockBody.Set("mqSet", mqArr);
        byte[] lockPayload = Bytes(lockBody.Dump());
        long slaveOk = -1;
        try
        {
            RemotingCommand resp = fx.Admin.Client().InvokeSyncRaw(fx.Slave,
                RequestCode.LockBatchMq, null, lockPayload, true, 3000);
            string respText = Encoding.UTF8.GetString(resp.Body ?? Array.Empty<byte>());
            if (Json.TryParse(respText, out JsonValue root, out _) && root is not null)
            {
                JsonValue okSet = root.Get("lockOKMQSet");
                if (okSet.IsArray)
                {
                    slaveOk = okSet.Size();
                }
            }
        }
        catch (Exception e)
        {
    Console.WriteLine("    (the slave's lock request threw: " + e.Message + ")");
        }

    Check("S5e2 control: naming the slave hands the lock out (an empty set is not the broker refusing)", slaveOk == 1,
            "slave lockOKMQSet=" + slaveOk);

        // The lock taken by the control leg has to be given back (nobody will release it in
        // the slave's lock manager)
        try
        {
            fx.Admin.Client().InvokeSyncRaw(fx.Slave, RequestCode.UnlockBatchMq,
                null, lockPayload, true, 3000);
        }
        catch (Exception)
        {
            // a release failure does not affect the assertions
        }

        sw.Restart();
        try
        {
            fx.Admin.Client().UnlockBatchMq(group, clientId, lockMqs, 3000);
        }
        catch (Exception e)
        {
         Console.WriteLine("    (UnlockBatchMq threw: " + e.Message + ")");
        }

        double unlockMs = sw.Elapsed.TotalMilliseconds;
        Check("S5e3 unlocking is skipped just as quietly (no throw, nothing sent)", unlockMs < LocalBudgetMs,
            unlockMs.ToString("F0", CultureInfo.InvariantCulture) + "ms");

        Console.WriteLine();
     Console.WriteLine("S5f (E) POP pull: locally reports \"The broker[" + BrokerName + "] not exist\" (no wire)");
        string popText;
        bool popFailed = false;
        bool popIsClientError = false;
        sw.Restart();
        try
        {
            PopResult popResult = fx.Admin.Client().PopMessage(group, topic, 0, 1, 30000, 100, 0);
            popText = "Ok(status=" + popResult.Status + ")";
        }
        catch (MQClientException e)
        {
            popFailed = true;
            popIsClientError = true;
            popText = e.Message;
        }
        catch (Exception e)
        {
            popFailed = true;
            popText = e.GetType().Name + ": " + e.Message;
        }

        double popMs = sw.Elapsed.TotalMilliseconds;
        Check("S5f inside the stop window the POP locally reports \"The broker[" + BrokerName + "] not exist\" (not an error returned by the slave)",
            popFailed && popIsClientError
            && popText == "The broker[" + BrokerName + "] not exist"
            && popMs < LocalBudgetMs,
            popMs.ToString("F0", CultureInfo.InvariantCulture) + "ms " + popText);

        Console.WriteLine();
   Console.WriteLine("S5g (F) offset read: a cold instance refreshes the route once and then relaxes to the slave");
        var cold = new MQClientInstance("pr_master_cs_cold_" + fx.Stamp,
            new List<string> { fx.Namesrv });
        try
        {
            string coldDetail;
            bool coldOk = false;
            try
            {
                bool found = cold.QueryConsumerOffset(group,
                    new MessageQueue(topic, BrokerName, 0), out long coldOffset, 5000);
                coldOk = true;
                coldDetail = found
                    ? "offset=" + coldOffset.ToString(CultureInfo.InvariantCulture)
                    : "offset=None(QUERY_NOT_FOUND)";
            }
            catch (Exception e)
            {
                coldDetail = e.GetType().Name + ": " + e.Message;
            }

   Check("S5g a cold instance's offset read does not error: refresh the route -> fall back to the slave and let the broker answer", coldOk, coldDetail);
        }
        finally
        {
            cold.Shutdown();
        }

        // ---------- S6 (C end to end) consuming inside the stop window ----------
        Console.WriteLine();
        Console.WriteLine("S6 a push consumer started inside the stop window: " + Queues + " queues + receiving the " + Queues + " seeded messages from the slave");
        var sink = new Sink();
        DefaultMQPushConsumer consumer = fx.BuildConsumer(group, sink);
        try
        {
            consumer.Start();
        }
        catch (Exception e)
        {
            Check("S6 consumer start", false, e.Message);
            return;
        }

        fx.Consumer = consumer;
        try
        {
            List<MessageQueue> subsInWindow = consumer.Client().GetTopicSubscribeInfo(topic);
     Check("S6a the consumer started inside the window also has " + Queues + " queues in its own subscribe info", subsInWindow.Count == Queues,
                "queues=" + FmtQueues(subsInWindow));
        }
        catch (Exception e)
        {
         Check("S6a the consumer started inside the window also has " + Queues + " queues in its own subscribe info", false, e.Message);
        }

        var expected = new List<string>(seeded);
        expected.Sort();
        bool gotAll = WaitUntil(() =>
        {
            List<string> got = sink.Bodies();
            got.Sort();
            return got.SequenceEqual(expected);
        }, 60000);
      Check("S6 all " + Queues + " received from the slave while the master is stopped", gotAll,
            "got=[" + string.Join(", ", sink.Bodies()) + "]");

        // ---------- S7 the negative control (bring the master back first) ----------
        Console.WriteLine();
        Console.WriteLine("S7 negative control: once the master is back the publish queues recover and sending recovers");
        (bool restarted, string startOut) = BrokerCtl(script, "start");
        if (!restarted)
        {
       Check("S7 the master came back", false, startOut);
        }

        List<MessageQueue> queuesBack = new();
        bool back = WaitUntil(() =>
        {
            try
            {
                TopicPublishInfo info = fx.Admin.Client().GetTopicPublishInfo(topic, false);
                if (info.MsgQueueList.Count == Queues)
                {
                    queuesBack = info.MsgQueueList;
                    return true;
                }
            }
            catch (Exception)
            {
                // not recovered yet: keep waiting
            }

            return false;
        }, 60000);
     Check("S7 the publish queues recovered to " + Queues, back, "queues=" + FmtQueues(queuesBack));

        // Neither failed send left a message on the broker: each seeded queue's maxOffset is
   // still 1
        var after = new List<(int QueueId, long Offset)>();
        foreach (MessageQueue mq in back ? queuesBack : queues)
        {
            try
            {
                after.Add((mq.QueueId, fx.MaxOffset(mq)));
            }
            catch (Exception e)
            {
        Console.WriteLine("    (the maxOffset query failed: " + e.Message + ")");
            }
        }

     Check("S7b the two failed sends left no messages (maxOffset is still 1)",
            after.Count > 0 && after.All(r => r.Offset == 1), "maxOffset=" + FmtOffsets(after));

        string backText;
        bool backOk = false;
        try
        {
            SendResult r = fx.Producer.Send(new Message(topic, Bytes("pr-master-back")), 20000);
            backOk = r.SendStatus == SendStatus.SendOk;
            backText = "status=" + r.SendStatus + " msgId=" + r.MsgId;
        }
        catch (Exception e)
        {
            backText = e.GetType().Name + ": " + e.Message;
        }

      Check("S7c sending recovered with SEND_OK", backOk, backText);
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        string master = args.Length > 1 ? args[1] : "127.0.0.1:10911";
        string slave = args.Length > 2 ? args[2] : "127.0.0.1:10931";
        string script = ScriptPath();

        Console.WriteLine("broker_ctl=" + script + " namesrv=" + namesrv
                          + " master=" + master + " slave=" + slave);
        (bool up, string upOut) = BrokerCtl(script, "status");
        if (!up)
        {
  Console.WriteLine("the master is not running: start namesrv + master + slave first following the local cluster runbook (" + upOut + ")");
       Check("the precondition master is UP", false, upOut);
            return Report();
        }

        var fx = new Fixture();
        try
        {
            fx.Start(namesrv, master, slave);
            Scenario(fx, script);
        }
        catch (Exception e)
        {
       Check("an unexpected exception occurred during the integration", false, e.ToString());
        }
        finally
        {
            fx.Cleanup(script);
        }

        return Report();
    }

    private static int Report()
    {
        Console.WriteLine();
        Console.WriteLine("############ PASS=" + _pass + " FAIL=" + _fail + " ############");
        return _fail == 0 ? 0 : 1;
    }
}
