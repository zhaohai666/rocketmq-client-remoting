// **Real cluster** message type integration of the C# client.
//
// Covers 11 message capabilities, all against a real nameServer + broker:
//   1. a synchronous ordinary message (Send -> SEND_MESSAGE_V2, persisted and read back)
//   2. an async send (sendAsync + SendCallback)
//   3. a oneway send (SendOneway, fires and does not wait for a response; only the read-back
//      count can corroborate it)
//   4. an orderly message (sendBySelector routes the same key to the same queue + orderly
//      consumption preserves the order)
//   5. a tagged message + server-side tag filtering
//   6. user property pass-through
//   7. a delayed message (setDelayTimeLevel plus verifying store_ts - born_ts >= 3000ms)
//   8. a scheduled message (the TIMER_DELIVER_MS property; the broker puts it in the timer
//      store's secondary storage and delivers it later, again verifying the delivery delay)
//   9. a keyed message + server-side lookup by key (QUERY_MESSAGE)
//  10. a transactional message (the two-phase commit path; the full chain is in
//      RunTransaction) + consumable after being persisted
//  11. a batch message (SendBatch -> SEND_BATCH_MESSAGE(320); the broker delivers it as N
//      independent messages with contiguous offsets)
//  In addition: consumer heartbeat registration (HEART_BEAT)
//
// This program does not start a cluster itself; the caller must start nameServer(9876) +
// broker(10911) first with autoCreateTopicEnable=true. Usage (called by Program in the
// form "message-types [namesrv] [ak] [sk]").
//
// When accessKey/secretKey are passed (a cluster with the broker's authenticationEnabled on):
//   - **every** client this program brings up (producer / push consumer / admin) gets the ACL
//     credentials attached;
//   - a probe producer without credentials runs first and its send is asserted to be
//     rejected (NO_PERMISSION=16) -- without this reverse assertion, being all green on a
//     cluster without auth enabled would just be a false pass.
// Without credentials the behaviour is exactly as in the old version (the plain-text cluster
// harness still works).
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

/// <summary>Real cluster integration of 11 message capabilities (optional ACL credentials plus the no-credentials reverse assertion).</summary>
internal static class LiveMessageTypes
{
    private static string _gNamesrv = "127.0.0.1:9876";
    private static string _gPrefix = string.Empty;
    // ACL credentials: they only take effect when both are non-empty; when empty no client
    // gets a hook and the behaviour is exactly as in the old version.
    private static string _gAk = string.Empty;
    private static string _gSk = string.Empty;
    private static bool WithAcl => _gAk.Length > 0 && _gSk.Length > 0;
    private static readonly List<(string Name, bool Ok)> Results = new();
    private static int _gPass;
    private static int _gFail;

    // The broker's uniform response code for an auth failure (see the acl cases in this directory).
    private const int NoPermission = 16;

    // Attaches the ACL credentials to every kind of client this program brings up. The
    // SetCredentials semantics are the same for all three: bound before Start() and
    // registered into the transport layer's single hook slot by RequestHooks.Compose during
    // Start().
    private static void ApplyCreds(DefaultMQProducer prod)
    {
        if (WithAcl) prod.SetCredentials(_gAk, _gSk);
    }

    private static void ApplyCreds(DefaultMQPushConsumer consumer)
    {
        if (WithAcl) consumer.SetCredentials(_gAk, _gSk);
    }

    private static void ApplyCreds(DefaultMQAdminExt admin)
    {
        if (WithAcl) admin.SetCredentials(_gAk, _gSk);
    }

    private static void Check(string name, bool ok, string detail = "")
    {
        Results.Add((name, ok));
        if (ok) ++_gPass;
        else ++_gFail;
        Console.WriteLine("[" + (ok ? "PASS" : "FAIL") + "] " + name + (detail.Length > 0 ? "  " + detail : string.Empty));
    }

    private static string Bytes2Str(byte[] b) => Encoding.UTF8.GetString(b);

    private static byte[] Str2Bytes(string s) => Encoding.UTF8.GetBytes(s);

    // ---------------- listeners ----------------

    private sealed class CollectingListenerConcurrently : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<MessageExt> _msgs = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext ctx)
        {
            lock (_lk) _msgs.AddRange(msgs);
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public List<MessageExt> Snapshot()
        {
            lock (_lk) return new List<MessageExt>(_msgs);
        }
    }

    private sealed class CollectingListenerOrderly : IMessageListenerOrderly
    {
        private readonly object _lk = new();
        private readonly List<MessageExt> _msgs = new();

        public bool Orderly() => true;

        public ConsumeOrderlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext ctx)
        {
            lock (_lk) _msgs.AddRange(msgs);
            return ConsumeOrderlyStatus.Success;
        }

        public List<MessageExt> Snapshot()
        {
            lock (_lk) return new List<MessageExt>(_msgs);
        }
    }

    private sealed class CollectingCallback : ISendCallback
    {
        private int _ok;
        private int _err;
        private readonly object _lk = new();
        private readonly List<SendResult> _results = new();
        private readonly List<Exception> _errors = new();

        public void OnSuccess(SendResult result)
        {
            Interlocked.Increment(ref _ok);
            lock (_lk) _results.Add(result);
        }

        public void OnException(Exception error)
        {
            Interlocked.Increment(ref _err);
            lock (_lk) _errors.Add(error);
        }

        public int Ok() => _ok;
        public int Err() => _err;

        public List<SendResult> Results()
        {
            lock (_lk) return new List<SendResult>(_results);
        }

        public List<string> Errors()
        {
            lock (_lk) return _errors.Select(e => e.Message).ToList();
        }
    }

    private sealed class CommitTxListener : ITransactionListener
    {
        public LocalTransactionState ExecuteLocalTransaction(Message msg, string arg) =>
            LocalTransactionState.CommitMessage;

        public LocalTransactionState CheckLocalTransaction(MessageExt msg) =>
            LocalTransactionState.CommitMessage;
    }

    /// <summary>The local transaction rolls back: the broker must not deliver the half message.</summary>
    private sealed class RollbackTxListener : ITransactionListener
    {
        public LocalTransactionState ExecuteLocalTransaction(Message msg, string arg) =>
            LocalTransactionState.RollbackMessage;

        public LocalTransactionState CheckLocalTransaction(MessageExt msg) =>
            LocalTransactionState.RollbackMessage;
    }

    /// <summary>
   /// The local transaction returns Unknow and Commit is only decided when the broker checks
    /// back.
    /// CheckCalls exists to prove the broker **really called back** (otherwise "eventually
    /// received" could just be the ordinary message path).
    /// </summary>
    private sealed class UnknownThenCommitTxListener : ITransactionListener
    {
        private int _checkCalls;

        public int CheckCalls => Volatile.Read(ref _checkCalls);

        public LocalTransactionState ExecuteLocalTransaction(Message msg, string arg) =>
            LocalTransactionState.Unknow;

        public LocalTransactionState CheckLocalTransaction(MessageExt msg)
        {
            Interlocked.Increment(ref _checkCalls);
            return LocalTransactionState.CommitMessage;
        }
    }

    // ---------------- consumption helpers ----------------

    // Waits actively until expect messages arrive (or at most durationSec seconds), then
  // closes the consumer and returns the messages received.
    private static List<MessageExt> RunConsumer(string topic, string subExpr, int durationSec,
        bool orderly, string groupSuffix, int expect = 0, int pullTimeout = 3000, int pullSuspend = 1000)
    {
        var conc = new CollectingListenerConcurrently();
        var ord = new CollectingListenerOrderly();
        IMessageListener listener = orderly ? ord : conc;

        var consumer = new DefaultMQPushConsumer(_gPrefix + "_" + groupSuffix)
        {
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
            PullTimeoutMillis = pullTimeout,
            PullSuspendTimeoutMillis = pullSuspend,
        };
        consumer.SetNamesrvAddr(_gNamesrv);
        ApplyCreds(consumer);
        consumer.Subscribe(topic, subExpr);
        consumer.SetMessageListener(listener);
        consumer.Start();

        var deadline = DateTime.UtcNow.AddSeconds(durationSec);
        while (DateTime.UtcNow < deadline)
        {
            int got = orderly ? ord.Snapshot().Count : conc.Snapshot().Count;
            if (expect > 0 && got >= expect) break;
            Thread.Sleep(100);
        }

        List<MessageExt> msgs = orderly ? ord.Snapshot() : conc.Snapshot();
        consumer.Shutdown();
        return msgs;
    }

    // Wait for the broker's registration with the nameServer to complete (avoiding the race
  // where the port is just opened but the registration has not landed)
    private static List<string> WaitBroker(DefaultMQProducer prod)
    {
        var addrs = new List<string>();
        for (int i = 0; i < 40; ++i)
        {
            try
            {
                List<MessageQueue> mqs = prod.FetchPublishMessageQueues(MixAll.DefaultTopic);
                foreach (MessageQueue mq in mqs)
                {
                    string a = prod.Client().BrokerAddrOf(mq.BrokerName);
                    if (a.Length > 0 && !addrs.Contains(a)) addrs.Add(a);
                }

                if (addrs.Count > 0) return addrs;
            }
            catch (Exception)
            {
   // the route is not ready yet, keep waiting
            }

            Thread.Sleep(1000);
        }

        return addrs;
    }

    // ---------------- entry point ----------------

    public static int Run(string[] args)
    {
        if (args.Length >= 1) _gNamesrv = args[0];
        if (args.Length >= 3)
        {
            _gAk = args[1];
            _gSk = args[2];
        }

        long stamp = UtilAll.CurrentTimeSeconds();
        _gPrefix = "MTDotnet_" + stamp;

        Console.WriteLine("=== C# client message type integration (real cluster) ===");
        Console.WriteLine("namesrv = " + _gNamesrv + "  prefix = " + _gPrefix +
                          "  acl = " + (WithAcl ? "on(ak=" + _gAk + ")" : "off"));

        // ---------- 0. the reverse assertion: a send without credentials must be rejected (NO_PERMISSION=16) ----------
     // This half only runs when credentials were passed: it exists to puncture the false
   // pass of "the cluster actually has auth disabled".
        // The probe send targets TBW102 (every broker registers the default topic on
    // startup, so the route always resolves; against a brand new topic the missing route
        // would blow up locally and never reach the broker's auth step).
        // The probe producer is started **first**: in this port a client's Start()
        // rewrites the default instanceName to <pid>#<nanoTime>, and each client owns its
    // own MQClientInstance and transport hook slot, so there is no first-come-first-served
        // cross-talk to begin with; running it first-started-first-stopped is safe under
        // both instance semantics anyway.
        if (WithAcl)
        {
            var probe = new DefaultMQProducer(_gPrefix + "_probe")
            {
                NamesrvAddr = _gNamesrv,
                SendMsgTimeout = 5000,
            };
            bool rejected = false;
            string probeDetail;
            try
            {
                probe.Start();
                try
                {
                    probe.Send(new Message(MixAll.DefaultTopic, Str2Bytes("no-cred-probe")));
        probeDetail = "unexpectedly succeeded (the cluster may not have auth enabled)";
                }
                catch (MQBrokerException e)
                {
                    probeDetail = "code=" + e.ResponseCode.ToString(CultureInfo.InvariantCulture)
                                  + " " + e.ResponseMessage;
                    rejected = e.ResponseCode == NoPermission;
                }
                catch (Exception e)
                {
           // Some admin paths splice the underlying exception into the message text, so
    // fall back to identifying it by the code's text.
                    probeDetail = e.Message;
                    rejected = probeDetail.Contains("CODE: "
                                 + NoPermission.ToString(CultureInfo.InvariantCulture), StringComparison.Ordinal);
                }
            }
            catch (Exception e)
            {
                probeDetail = "probe start throw: " + e.Message;
            }
            finally
            {
                probe.Shutdown();
            }

  Check("reverse: a send without credentials is rejected (NO_PERMISSION=16)", rejected, probeDetail);
            // Wait a beat after shutting down, to make sure the probe instance's resources are
   // fully returned so they cannot affect the real clients afterwards.
            Thread.Sleep(1000);
        }

        var prod = new DefaultMQProducer(_gPrefix + "_producer")
        {
            NamesrvAddr = _gNamesrv,
            SendMsgTimeout = 5000,
        };
        ApplyCreds(prod);
        try
        {
            prod.Start();
        }
        catch (Exception e)
        {
    Check("the producer started", false, e.Message);
            return 1;
        }

        List<string> brokers = WaitBroker(prod);
        if (brokers.Count == 0)
        {
            Check("the cluster liveness probe", false, "the nameServer has no broker registered");
            prod.Shutdown();
            return 1;
        }

        Check("the cluster liveness probe", true, "brokers=" + string.Join(",", brokers));

        // Pre-create every topic this case uses: a new topic only enters the route after the
    // broker's registerNameServerPeriod round (one every 30s), and until then the consumer's
  // subscription just spins doing nothing.
        {
            var admin = new DefaultMQAdminExt();
            admin.SetNamesrvAddr(_gNamesrv);
            ApplyCreds(admin);
            admin.SetTimeoutMillis(10000);
            admin.Start();
            try
            {
                foreach (string suffix in new[]
                         { "Normal", "Async", "Oneway", "Order", "Tag", "Prop", "Delay", "Timer", "Key", "Tx",
                           "TxRollback", "TxCheck", "Batch", "Hb" })
                {
                    admin.CreateTopic(MixAll.DefaultTopic, _gPrefix + "_" + suffix, 4);
                }
                Thread.Sleep(1000);
            }
            catch (Exception e)
            {
   Console.WriteLine("  pre-creating the topic failed (falling back to auto-creation): " + e.Message);
            }
            admin.Shutdown();
        }

        long t0 = UtilAll.CurrentTimeMillis();

      // ---------- 1. a synchronous ordinary message ----------
        {
            string topic = _gPrefix + "_Normal";
            var bodies = new List<string>();
            bool allOk = true;
            for (int i = 0; i < 3; ++i)
            {
                string body = "normal-" + i.ToString(CultureInfo.InvariantCulture);
                bodies.Add(body);
                try
                {
                    SendResult sr = prod.Send(new Message(topic, Str2Bytes(body)));
                    if (sr.SendStatus != SendStatus.SendOk || string.IsNullOrEmpty(sr.MsgId)) allOk = false;
                }
                catch (Exception e)
                {
                    allOk = false;
                    Console.WriteLine("  sync send throw: " + e.Message);
                }
            }

            List<MessageExt> run = RunConsumer(topic, "*", 12, false, "normal", expect: 3);
            var got = run.Select(m => Bytes2Str(m.Body)).OrderBy(x => x, StringComparer.Ordinal).ToList();
            bool readBack = got.SequenceEqual(bodies.OrderBy(x => x, StringComparer.Ordinal));
       Check("the synchronous ordinary send is SendOk", allOk);
    Check("the 3/3 synchronous ordinary messages are persisted and read back", readBack,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 2. an async send ----------
        {
            string topic = _gPrefix + "_Async";
            var cb = new CollectingCallback();
            Message msg = new(topic, Str2Bytes("async-hello"));
            try
            {
                prod.SendAsync(msg, cb);
            }
            catch (Exception e)
            {
          Check("the async send sendAsync", false, "throw: " + e.Message);
            }

            for (int i = 0; i < 200 && cb.Ok() + cb.Err() == 0; ++i)
            {
                Thread.Sleep(25);
            }

            List<SendResult> rs = cb.Results();
            bool ok = cb.Ok() == 1 && cb.Err() == 0 && rs.Count > 0 && rs[0].SendStatus == SendStatus.SendOk;
   Check("the async send sendAsync", ok, "ok=" + cb.Ok().ToString(CultureInfo.InvariantCulture)
                  + " err=" + cb.Err().ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 3. a oneway send (SendOneway)----------
  // A oneway does not wait for a broker response, so this side can only prove "it went out
    // and the broker really persisted it": the criterion is the read-back count on the
  // consume side (the only type in this whole program allowed to rely on the read-back
  // count alone).
        {
            string topic = _gPrefix + "_Oneway";
            bool threw = false;
            try
            {
                for (int i = 0; i < 3; ++i)
                {
                    prod.SendOneway(new Message(topic, Str2Bytes(
                        "oneway-" + i.ToString(CultureInfo.InvariantCulture))));
                }
            }
            catch (Exception e)
            {
                threw = true;
                Console.WriteLine("  oneway send throw: " + e.Message);
            }

            List<MessageExt> run = RunConsumer(topic, "*", 15, false, "oneway", expect: 3);
            Check("the oneway send SendOneway(3 messages) is persisted and read back", !threw && run.Count == 3,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture) + "/3");
        }

        // ---------- 4. an orderly message: the same key routes to the same queue + orderly consumption preserves the order ----------
        List<byte[]> bodiesOrder = new();
        {
            string topic = _gPrefix + "_Order";
            var selector = new SelectMessageQueueByHash();
            var qids = new List<int>();
            for (int i = 0; i < 10; ++i)
            {
                byte[] body = Str2Bytes(string.Format(CultureInfo.InvariantCulture, "ord-{0:00}", i));
                bodiesOrder.Add(body);
                SendResult sr = prod.SendBySelector(new Message(topic, body), selector, "shard-A");
                if (!qids.Contains(sr.MessageQueue.QueueId)) qids.Add(sr.MessageQueue.QueueId);
            }

      Check("orderly send: the same key routes to the same queue", qids.Count == 1,
                "distinct_queue_ids=" + (qids.Count == 0 ? "?" : string.Join(",", qids)));

            List<MessageExt> run = RunConsumer(topic, "*", 12, /*orderly=*/true, "order", /*expect=*/10);
            List<string> recv = run.Select(m => Bytes2Str(m.Body)).ToList();
            List<string> expect = bodiesOrder.Select(Bytes2Str).ToList();
            bool seqOk = recv.SequenceEqual(expect);
            bool offsetOk = true;
            for (int i = 1; i < run.Count; ++i)
            {
                if (run[i].QueueOffset <= run[i - 1].QueueOffset) offsetOk = false;
            }

            string orderDetail = "received=" + run.Count.ToString(CultureInfo.InvariantCulture) + "/10 seq_ok="
                                 + (seqOk ? "1" : "0") + " offset_monotonic=" + (offsetOk ? "1" : "0");
            if (run.Count > 0)
            {
                orderDetail += " first=" + recv[0] + " last=" + recv[run.Count - 1];
            }

        Check("orderly consumption preserves the order", run.Count == 10 && seqOk && offsetOk, orderDetail);
        }

     // ---------- 5. a tagged message + server-side tag filtering ----------
        {
            string topic = _gPrefix + "_Tag";
            for (int i = 0; i < 3; ++i)
            {
                Message m = new(topic, Str2Bytes("tagA-" + i)) { Tags = "TagA" };
                prod.Send(m);
            }

            for (int i = 0; i < 3; ++i)
            {
                Message m = new(topic, Str2Bytes("tagB-" + i)) { Tags = "TagB" };
                prod.Send(m);
            }

            List<MessageExt> run = RunConsumer(topic, "TagA", 12, false, "tag");
            bool allA = run.All(m => m.Tags == "TagA");
            Check("tag-filtered consumption (only TagA received)", run.Count == 3 && allA,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 6. user property pass-through ----------
        {
            string topic = _gPrefix + "_Prop";
            for (int i = 0; i < 3; ++i)
            {
                Message m = new(topic, Str2Bytes("prop-" + i)) { Tags = "P" };
                m.SetUserProperty("city", "Hangzhou");
                m.SetUserProperty("env", "prod");
                prod.Send(m);
            }

            List<MessageExt> run = RunConsumer(topic, "*", 12, false, "prop");
            bool okCity = run.All(m => m.GetUserProperty("city") == "Hangzhou");
            bool okEnv = run.All(m => m.GetUserProperty("env") == "prod");
   Check("user property pass-through (city=Hangzhou, env=prod)", okCity && okEnv && run.Count == 3,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 7. a delayed message (delay level)----------
        {
            string topic = _gPrefix + "_Delay";
            prod.Send(new Message(topic, Str2Bytes("normal-now")));
            Message dm = new(topic, Str2Bytes("delayed-5s")) { DelayTimeLevel = 2 }; // level2 = 5s
            prod.Send(dm);

            List<MessageExt> run = RunConsumer(topic, "*", 20, false, "delay", /*expect=*/2);
            var delayed = new List<MessageExt>();
            var normal = new List<MessageExt>();
            foreach (MessageExt m in run)
            {
                if (Bytes2Str(m.Body) == "delayed-5s") delayed.Add(m);
                else if (Bytes2Str(m.Body) == "normal-now") normal.Add(m);
            }

     Check("the delayed message is eventually delivered", delayed.Count > 0,
                "delayed received=" + delayed.Count.ToString(CultureInfo.InvariantCulture)
                + " normal=" + normal.Count.ToString(CultureInfo.InvariantCulture));
            if (delayed.Count > 0)
            {
                long drift = delayed[0].StoreTimestamp - delayed[0].BornTimestamp;
  Check("the delay took effect (store_ts-born_ts>=3000ms)", drift >= 3000,
                    "drift=" + drift.ToString(CultureInfo.InvariantCulture) + "ms");
            }
            else
            {
     Check("the delay took effect (store_ts-born_ts>=3000ms)", false, "no delayed message to verify");
            }
        }

   // ---------- 8. a scheduled message (TIMER_DELIVER_MS)----------
   // A different mechanism from delay level: the broker puts a message carrying
        // TIMER_DELIVER_MS into the timer store (a secondary storage outside
   // SCHEDULE_TOPIC_XXXX) and delivers it back to the target queue when it is due.
        // On the client side all that is needed is to write the property into the user properties
   // (SetDeliverTimeMs is the convenient entry point for it), and the criterion is still the
  // store_ts - born_ts of the message read back: if the timer did not take effect the drift
 // would only be milliseconds.
        {
            string topic = _gPrefix + "_Timer";
            long deliverAt = UtilAll.CurrentTimeMillis() + 5000;
            Message tm = new(topic, Str2Bytes("timer-5s"));
            tm.SetDeliverTimeMs(deliverAt);
            prod.Send(tm);

            List<MessageExt> run = RunConsumer(topic, "*", 25, false, "timer", expect: 1);
            var hit = run.Where(m => Bytes2Str(m.Body) == "timer-5s").ToList();
        Check("the scheduled message (TIMER_DELIVER_MS) is eventually delivered", hit.Count > 0,
                "received=" + hit.Count.ToString(CultureInfo.InvariantCulture) + "/1");
            if (hit.Count > 0)
            {
                long drift = hit[0].StoreTimestamp - hit[0].BornTimestamp;
          Check("the timer took effect (store_ts-born_ts>=4000ms)", drift >= 4000,
                    "drift=" + drift.ToString(CultureInfo.InvariantCulture) + "ms");
            }
            else
            {
     Check("the timer took effect (store_ts-born_ts>=4000ms)", false, "no scheduled message to verify");
            }
        }

      // ---------- 9. a keyed message + lookup by key ----------
        {
            string topic = _gPrefix + "_Key";
            string key = "MTKEY_" + stamp.ToString(CultureInfo.InvariantCulture);
            Message m = new(topic, Str2Bytes("key-msg-payload")) { Keys = key };
            prod.Send(m);
            Thread.Sleep(1000);

            long begin = t0 - 120000;
            long end = UtilAll.CurrentTimeMillis() + 120000;
            List<MessageExt> found = prod.QueryMessage(topic, key, 10, begin, end);
            bool hit = found.Any(x => Bytes2Str(x.Body) == "key-msg-payload");
      Check("lookup by key (query_message)", hit, "returned=" + found.Count.ToString(CultureInfo.InvariantCulture));
        }

    // ---------- 10. a transactional message: COMMIT (half message -> local transaction commit -> consumable after being persisted)----------
        {
            string topic = _gPrefix + "_Tx";
            var listener = new CommitTxListener();
            string stateStr;
            bool txOk = false;
            try
            {
                TransactionSendResult tsr = prod.SendMessageInTransaction(
                    new Message(topic, Str2Bytes("tx-commit")), listener);
                stateStr = LocalTransactionStateNames.Name(tsr.LocalTransactionState);
                txOk = tsr.SendStatus == SendStatus.SendOk
                    && tsr.LocalTransactionState == LocalTransactionState.CommitMessage;
            }
            catch (Exception e)
            {
                stateStr = "throw: " + e.Message;
            }

            Check("transaction-COMMIT send state", txOk, "state=" + stateStr);

            List<MessageExt> run = RunConsumer(topic, "*", 10, false, "tx");
            bool consumed = run.Any(m => Bytes2Str(m.Body) == "tx-commit");
        Check("transaction-COMMIT is persisted and consumable", consumed,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 10.2 ROLLBACK ----------
        {
            string topic = _gPrefix + "_TxRollback";
            var listener = new RollbackTxListener();
            string stateStr;
            bool txOk = false;
            try
            {
                TransactionSendResult tsr = prod.SendMessageInTransaction(
                    new Message(topic, Str2Bytes("tx-rollback")), listener);
                stateStr = LocalTransactionStateNames.Name(tsr.LocalTransactionState);
                txOk = tsr.SendStatus == SendStatus.SendOk
                    && tsr.LocalTransactionState == LocalTransactionState.RollbackMessage;
            }
            catch (Exception e)
            {
                stateStr = "throw: " + e.Message;
            }

        Check("transaction-ROLLBACK send state", txOk, "state=" + stateStr);

            List<MessageExt> run = RunConsumer(topic, "*", 10, false, "txrollback");
            bool consumed = run.Any(m => Bytes2Str(m.Body) == "tx-rollback");
         Check("transaction-ROLLBACK is not delivered", !consumed,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 10.3 UNKNOW + the broker checking back ----------
        {
            string topic = _gPrefix + "_TxCheck";
            var listener = new UnknownThenCommitTxListener();
            string stateStr;
            bool txOk = false;
            try
            {
                TransactionSendResult tsr = prod.SendMessageInTransaction(
                    new Message(topic, Str2Bytes("tx-check")), listener);
                stateStr = LocalTransactionStateNames.Name(tsr.LocalTransactionState);
                txOk = tsr.SendStatus == SendStatus.SendOk
                    && tsr.LocalTransactionState == LocalTransactionState.Unknow;
            }
            catch (Exception e)
            {
                stateStr = "throw: " + e.Message;
            }

     Check("transaction-UNKNOW send state", txOk, "state=" + stateStr);

            long checkDeadline = UtilAll.CurrentTimeMillis() + 90_000;
            while (listener.CheckCalls == 0 && UtilAll.CurrentTimeMillis() < checkDeadline)
            {
                Thread.Sleep(1000);
            }
            List<MessageExt> run = RunConsumer(topic, "*", 10, false, "txcheck");
            bool consumed = run.Any(m => Bytes2Str(m.Body) == "tx-check");
            int checks = listener.CheckCalls;
            Check("transaction-UNKNOW triggered the broker to check back", checks > 0,
                "checkLocalTransaction_calls=" + checks.ToString(CultureInfo.InvariantCulture));
            Check("transaction-UNKNOW is eventually delivered after the check-back", consumed,
                "received=" + run.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------- 11. a batch message (SEND_BATCH_MESSAGE 320)----------
    // After the request code switches from 310 to 320 it has to be proven on a real broker
    // that it really is handled as a "batch write": the criterion is that the consume side
  // receives N **individually complete** messages with contiguous queueOffsets. If the
    // broker stored the batch body as a single message (header.batch not taking effect), only
  // one message of concatenated bytes would arrive here.
        {
            string topic = _gPrefix + "_Batch";
            bool batchOk;
            string detail;
            try
            {
                var group = new List<Message>();
                for (int i = 0; i < 3; ++i)
                {
                    group.Add(new Message(topic, Str2Bytes("batch-" + i.ToString(CultureInfo.InvariantCulture))));
                }

                SendResult sr = prod.SendBatch(group, 5000);
                batchOk = sr.SendStatus == SendStatus.SendOk;
                detail = "status=" + sr.SendStatus;
            }
            catch (Exception e)
            {
                batchOk = false;
                detail = "throw: " + e.Message;
            }

            Check("the batch send SendBatch(3 messages)", batchOk, detail);

            List<MessageExt> msgs = RunConsumer(topic, "*", 15, false, "batch", expect: 3);
            var bodies = new HashSet<string>();
            var offsets = new List<long>();
            foreach (MessageExt m in msgs)
            {
                bodies.Add(Bytes2Str(m.Body));
                offsets.Add(m.QueueOffset);
            }

            offsets.Sort();
            bool contiguous = offsets.Count == 3;
            for (int i = 1; i < offsets.Count; ++i)
            {
                if (offsets[i] != offsets[i - 1] + 1) contiguous = false;
            }

            bool exact = true;
            for (int i = 0; i < 3; ++i)
            {
                if (!bodies.Contains("batch-" + i.ToString(CultureInfo.InvariantCulture))) exact = false;
            }

   Check("the batch messages were delivered by the broker as 3 independent messages", exact && contiguous,
                "received=" + msgs.Count.ToString(CultureInfo.InvariantCulture)
                + " offsets=" + string.Join(",", offsets));
        }

    // ---------- in addition: heartbeat registration ----------
        {
            string topic = _gPrefix + "_Hb";
            prod.Send(new Message(topic, Str2Bytes("hb-probe")));
            var listener = new CollectingListenerConcurrently();
            var cons = new DefaultMQPushConsumer(_gPrefix + "_hb")
            {
                ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
                PullTimeoutMillis = 3000,
                PullSuspendTimeoutMillis = 1000,
            };
            cons.SetNamesrvAddr(_gNamesrv);
            ApplyCreds(cons);
            cons.Subscribe(topic, "*");
            cons.SetMessageListener(listener);
            cons.Start();
            for (int i = 0; i < 40 && listener.Snapshot().Count < 1; ++i)
            {
                Thread.Sleep(250);
            }

            long hb = cons.HeartbeatCount;
            cons.Shutdown();
      Check("the consumer heartbeat registration (HEART_BEAT)", hb > 0,
                "heartbeat_ok=" + hb.ToString(CultureInfo.InvariantCulture)
                + " consumed=" + listener.Snapshot().Count.ToString(CultureInfo.InvariantCulture));
        }

        prod.Shutdown();

  // ---------- summary ----------
        Console.WriteLine();
   Console.WriteLine("================ C# message type integration summary ================");
        foreach ((string Name, bool Ok) r in Results)
        {
            Console.WriteLine("  [" + (r.Ok ? "PASS" : "FAIL") + "] " + r.Name);
        }

        Console.WriteLine("=====================================================");
        Console.WriteLine("  PASS=" + _gPass.ToString(CultureInfo.InvariantCulture)
                          + " FAIL=" + _gFail.ToString(CultureInfo.InvariantCulture));
        if (_gFail > 0)
        {
        Console.WriteLine("result: " + _gFail.ToString(CultureInfo.InvariantCulture) + " failures");
            return 1;
        }

        Console.WriteLine("result: everything passed (the C# client completed every message type send/receive against a real cluster)");
        return 0;
    }
}
