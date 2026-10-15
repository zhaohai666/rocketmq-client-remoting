// Usage: rmq pinned-guard [namesrv] (needs a local 5.5.1 cluster, autoCreateTopicEnable=true)
using System.Diagnostics;
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LivePinnedGuard
{
    private const string SyncWording = "message's topic not equal mq's topic";
    private const string AsyncWording =
        "Topic of the message does not match its target message queue";

   /// <summary>The upper bound on the negative leg's elapsed time: the guard is a pure string
    /// comparison, so 50ms on a real machine already leaves two orders of magnitude of
    /// slack.</summary>
    private const double LocalBudgetMs = 50.0;

    private static string _namesrv = "127.0.0.1:9876";

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

    private static string Join(IEnumerable<string> v) => "[" + string.Join(", ", v) + "]";

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

   /// <summary>A message with keys: (topic, tags, keys, body) is the only constructor with
    /// keys in C#.</summary>
    private static Message Keyed(string topic, string body, string keys) =>
        new(topic, null!, keys, Bytes(body));

    private static long NowMs() =>
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 100)
    {
        long deadline = NowMs() + timeoutMs;
        while (NowMs() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(intervalMs);
        }

        return pred();
    }

   /// <summary>Waits until maxOffset climbs to want before returning (the broker's
    /// ConsumeQueue dispatch is a few milliseconds slower than the SEND reply).</summary>
    private static long WaitOffset(DefaultMQProducer prod, MessageQueue mq, long want,
        int timeoutMs = 5000)
    {
        WaitUntil(() => prod.MaxOffset(mq) >= want, timeoutMs, 50);
        return prod.MaxOffset(mq);
    }

    private static MessageQueue QueueZero(DefaultMQProducer prod, string topic)
    {
        foreach (MessageQueue q in prod.FetchPublishMessageQueues(topic))
        {
            if (q.QueueId == 0) return q;
        }

        throw new InvalidOperationException("topic " + topic + " has no queue 0");
    }

    /// <summary>Runs one pinned synchronous send and returns (the exception or null, the
    /// elapsed milliseconds).</summary>
    private static (MQClientException? Err, double Ms) TimedSend(Func<SendResult> send)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            send();
            return (null, sw.Elapsed.TotalMilliseconds);
        }
        catch (MQClientException e)
        {
            return (e, sw.Elapsed.TotalMilliseconds);
        }
    }

    /// <summary>The terminal state of one async send (one record for success and one for the
   /// exception, the callback fires exactly once).</summary>
    private sealed class EventSink : ISendCallback
    {
        private readonly object _lk = new();
        private readonly List<(string Kind, SendResult? Result, Exception? Error)> _events = new();

        public void OnSuccess(SendResult sendResult)
        {
            lock (_lk) _events.Add(("ok", sendResult, null));
        }

        public void OnException(Exception error)
        {
            lock (_lk) _events.Add(("err", null, error));
        }

        public bool Done()
        {
            lock (_lk) return _events.Count > 0;
        }

        public (string Kind, SendResult? Result, Exception? Error) First()
        {
            lock (_lk) return _events.Count > 0 ? _events[0] : ("", null, null);
        }
    }

   /// <summary>Puts the keys of the positive leg's messages into a bag;
    /// CONSUME_FROM_FIRST_OFFSET keeps it from racing the send for the rebalance.</summary>
    private sealed class KeyListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<string> _keys = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                foreach (MessageExt m in msgs)
                {
                    _keys.Add(m.Keys);
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public List<string> Snapshot()
        {
            lock (_lk) return new List<string>(_keys);
        }
    }

    public static int Run(string[] args)
    {
        if (args.Length > 0 && args[0].Trim().Length > 0) _namesrv = args[0].Trim();
        string stamp = NowMs().ToString(CultureInfo.InvariantCulture);
        string topic = "PinGuard_" + stamp;
        string other = "PinGuardOther_" + stamp;
        string group = "G_pin_guard_" + stamp;
        const string ns = "ns1";
        string wTopic = ns + "%" + topic;
        Console.WriteLine("namesrv=" + _namesrv + " topic=" + topic + " other=" + other);

        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_namesrv);
        admin.Start();
        string brokerAddr;
        try
        {
            ClusterInfo cluster = admin.FetchBrokerClusterInfo();
            List<string> addrs = cluster.GetBrokerAddrs();
            if (addrs.Count == 0)
            {
      Check("the cluster liveness probe", false, "the nameServer has no broker registered");
                admin.Shutdown();
                return Report();
            }

            brokerAddr = addrs[0];
        }
        catch (Exception e)
        {
         Check("the cluster liveness probe", false, "fetchBrokerClusterInfo: " + e.Message);
            admin.Shutdown();
            return Report();
        }

        Check("the cluster liveness probe", true, "broker=" + brokerAddr);

        DefaultMQProducer? prod = null;
        DefaultMQProducer? nsProd = null;
        DefaultMQPushConsumer? push = null;
        var sink = new KeyListener();
        try
        {
            prod = new DefaultMQProducer("PG_pin_guard_" + stamp)
            {
                InstanceName = "pin-guard-" + stamp,
            };
            prod.NamesrvAddr = _namesrv;
            prod.Start();
            prod.CreateTopic(MixAll.DefaultTopic, topic, 4);
            prod.CreateTopic(MixAll.DefaultTopic, other, 4);
            if (!(WaitRoute(prod, topic) && WaitRoute(prod, other)))
            {
  Check("S0 both topics' routes are registered", false);
                return Report();
            }

            MessageQueue mq0 = QueueZero(prod, topic);
        // The negative leg's target queue: a **real** queue (queue 0 of another topic on the
        // same broker). A non-existent broker/queue would die on the route first, and then
  // you could not tell whether the topic or the address was what got rejected.
            MessageQueue mqOther = QueueZero(prod, other);
        Check("S0 both topics' routes are usable (the negative leg uses a real queue, so what is rejected is definitely the topic and not the address)",
                true, "A=" + mq0.BrokerName + "/" + mq0.QueueId
                + " B=" + mqOther.BrokerName + "/" + mqOther.QueueId);

        // The consumer starts first: CONSUME_FROM_FIRST_OFFSET, so it does not race the send for the rebalance timing
            push = new DefaultMQPushConsumer(group)
            {
                InstanceName = "pin-guard-live-" + stamp,
                ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
            };
            push.SetNamesrvAddr(_namesrv);
            push.Subscribe(topic, "*");
            push.SetMessageListener(sink);
            push.Start();

            // ---------------- S1 the positive leg: a pinned send to a real route queue ----------------
            Console.WriteLine("=== S1 a pinned send to a real route queue ===");
            string kSingle = "pinned-live-single";
            SendResult r1 = prod.Send(Keyed(topic, "s1-single", kSingle), mq0, 3000);
            Check("S1a the synchronous single pinned send is SEND_OK and landed on queue " + mq0.QueueId,
                r1.SendStatus == SendStatus.SendOk && r1.MessageQueue.QueueId == mq0.QueueId
                && r1.MessageQueue.Topic == topic,
                "status=" + SendStatusNames.Name(r1.SendStatus) + " mq=" + r1.MessageQueue);

            string kB1 = "pinned-live-b1";
            string kB2 = "pinned-live-b2";
            SendResult rb = prod.SendBatch(
                new List<Message> { Keyed(topic, "s1-b1", kB1), Keyed(topic, "s1-b2", kB2) },
                mq0, 3000);
        Check("S1b the synchronous batch pinned send is SEND_OK and on the same queue",
                rb.SendStatus == SendStatus.SendOk && rb.MessageQueue.QueueId == mq0.QueueId,
                "status=" + SendStatusNames.Name(rb.SendStatus) + " mq=" + rb.MessageQueue);

            long offAfterS1 = WaitOffset(prod, mq0, 3);
      // A batch lands on the consume queue **per sub-message** (the broker splits the
        // inner-batch apart after receiving it) => 2 sub messages move the offset by 2
  Check("S1c all three sub-messages really landed (maxOffset = 1 single + 2 batch sub-messages)",
                offAfterS1 == 3, "maxOffset=" + offAfterS1.ToString(CultureInfo.InvariantCulture));

            // ---------------- S2 the negative leg: a synchronous rejection, local and traceless ----------------
            Console.WriteLine("=== S2 a mismatched topic: rejected sub-millisecond locally, no trace on the broker ===");
            (MQClientException? e, double elapsed) = TimedSend(() =>
                prod.Send(Keyed(topic, "refused", "pinned-live-refused"), mqOther, 3000));
            Check("S2a the synchronous single send is rejected with the text verbatim as agreed by the protocol",
                e is not null && e.Message == SyncWording,
          Fmt(elapsed) + "ms " + (e is null ? "nothing was thrown" : e.Message));
        // The code of a local rejection is MQClientException's default 1 (UNKNOWN), not a code
        // returned by the broker
       Check("S2b the rejection is local: sub-millisecond and the response code is the client's default (not a timeout / not a broker remark)",
                e is not null && elapsed < LocalBudgetMs && e.ResponseCode == 1,
                Fmt(elapsed) + "ms code=" + (e?.ResponseCode.ToString(CultureInfo.InvariantCulture) ?? "n/a"));
            (MQClientException? e2, double elapsed2) = TimedSend(() =>
                prod.SendBatch(new List<Message> { Keyed(topic, "r1", ""), Keyed(topic, "r2", "") },
                    mqOther, 3000));
            Check("S2c the synchronous batch shares the same guard and the same message text",
                e2 is not null && e2.Message == SyncWording && elapsed2 < LocalBudgetMs,
        Fmt(elapsed2) + "ms " + (e2 is null ? "nothing was thrown" : e2.Message));
            long offA = prod.MaxOffset(mq0);
            long offB = prod.MaxOffset(mqOther);
            Check("S2d the wire counter-proof: A's maxOffset did not move at all and B has not a single message",
                offA == offAfterS1 && offB == 0, "A=" + offA + " B=" + offB);

            // ---------------- S3 the namespace: wrap is idempotent and only a genuinely different name is rejected ----------------
            Console.WriteLine("=== S3 the comparison under a namespace (neither ns%topic nor a bare topic is rejected by mistake) ===");
            nsProd = new DefaultMQProducer("PG_pin_guard_ns_" + stamp)
            {
                InstanceName = "pin-guard-ns-" + stamp,
            };
            nsProd.NamesrvAddr = _namesrv;
            nsProd.Namespace = ns;
            nsProd.Start();
            nsProd.CreateTopic(MixAll.DefaultTopic, wTopic, 4);
            Check("S3a the prefixed topic's route is usable", WaitRoute(nsProd, wTopic), "topic=" + wTopic);
            MessageQueue mqw = QueueZero(nsProd, wTopic);
            string kNs1 = "pinned-live-ns-q";
            string kNs2 = "pinned-live-ns-m";
            SendResult n1 = nsProd.Send(Keyed(topic, "ns-wrapped-queue", kNs1), mqw, 3000);
            Check("S3b the queue's topic already carries the ns prefix: wrap is idempotent, no mistaken rejection",
                n1.SendStatus == SendStatus.SendOk, "status=" + SendStatusNames.Name(n1.SendStatus));
            SendResult n2 = nsProd.Send(Keyed(wTopic, "ns-wrapped-message", kNs2), mqw, 3000);
            Check("S3c a message topic that already carries the prefix is let through too",
                n2.SendStatus == SendStatus.SendOk, "status=" + SendStatusNames.Name(n2.SendStatus));
            (MQClientException? e3, double elapsed3) = TimedSend(() =>
                nsProd.Send(Keyed(topic, "ns2-refused", ""),
                    new MessageQueue("ns2%" + topic, mqw.BrokerName, 0), 3000));
            Check("S3d only switching to the ns2% prefix is rejected (the control leg: what is rejected is the name, not \"having a prefix\")",
                e3 is not null && e3.Message == SyncWording && elapsed3 < LocalBudgetMs,
                Fmt(elapsed3) + "ms " + (e3 is null ? "nothing was thrown" : e3.Message));
            Check("S3e the two allowed legs really landed in ns1%topic (maxOffset=2)",
                WaitOffset(nsProd, mqw, 2) == 2,
                "maxOffset=" + nsProd.MaxOffset(mqw).ToString(CultureInfo.InvariantCulture));

            // ---------------- S4 async: the callback carries the async message text ----------------
            Console.WriteLine("=== S4 async single/batch: a rejection goes to the callback, an allowed one goes through the kernel ===");
            var refused1 = new EventSink();
            prod.SendAsync(Keyed(topic, "async-refused", "pinned-live-async-refused"),
                refused1, 3000, mqOther);
            bool okEv = WaitUntil(refused1.Done, 10000, 50);
            (string kind1, _, Exception? err1) = refused1.First();
            Check("S4a the async single send is rejected via the callback and the text is the async one",
                okEv && kind1 == "err" && err1 is not null && err1.Message.Contains(AsyncWording),
                kind1 + " " + (err1?.Message ?? ""));
            Check("S4b after the rejection maxOffset still does not move (async did not sneak a send out either)",
                prod.MaxOffset(mq0) == offAfterS1,
                "maxOffset=" + prod.MaxOffset(mq0).ToString(CultureInfo.InvariantCulture));

            var refused2 = new EventSink();
            prod.SendBatchAsync(new List<Message> { Keyed(topic, "abr1", ""), Keyed(topic, "abr2", "") },
                refused2, 3000, mqOther);
            WaitUntil(refused2.Done, 10000, 50);
            (string kind2, _, Exception? err2) = refused2.First();
            Check("S4c batch async shares the same message text and the same rejection path",
                kind2 == "err" && err2 is not null && err2.Message.Contains(AsyncWording),
                kind2 + " " + (err2?.Message ?? ""));

            string kAs = "pinned-live-async-single";
            string kAb1 = "pinned-live-async-b1";
            string kAb2 = "pinned-live-async-b2";
            var ok1 = new EventSink();
            var ok2 = new EventSink();
            prod.SendAsync(Keyed(topic, "async-ok", kAs), ok1, 3000, mq0);
            prod.SendBatchAsync(new List<Message> { Keyed(topic, "ab1", kAb1), Keyed(topic, "ab2", kAb2) },
                ok2, 3000, mq0);
            WaitUntil(() => ok1.Done() && ok2.Done(), 10000, 50);
            (string kindS, SendResult? resS, _) = ok1.First();
            (string kindB, SendResult? resB, _) = ok2.First();
            Check("S4d both allowed legs got SEND_OK",
                kindS == "ok" && resS?.SendStatus == SendStatus.SendOk
                && kindB == "ok" && resB?.SendStatus == SendStatus.SendOk,
                "single=" + kindS + " batch=" + kindB);
            long offAsync = WaitOffset(prod, mq0, offAfterS1 + 3);
        Check("S4e the async allowed leg really landed too (1 single + 2 batch sub-messages, maxOffset climbs by 3 more)",
                offAsync == offAfterS1 + 3,
                "maxOffset=" + offAsync.ToString(CultureInfo.InvariantCulture));

  Console.WriteLine("=== S5 a pinned oneway has no guard: the msg's own topic decides ===");
            string kOw = "pinned-live-oneway";
            prod.SendOneway(Keyed(topic, "oneway", kOw), mqOther);
            bool landed = WaitUntil(() => prod.MaxOffset(mq0) == offAsync + 1, 10000, 100);
            Check("S5a a pinned oneway has no guard: the request landed in A according to the msg's own topic",
                landed, "A maxOffset=" + prod.MaxOffset(mq0).ToString(CultureInfo.InvariantCulture));
        Check("S5b B, the topic the queue belonged to, has not a single message (this is a hole inherent to the oneway path, not a missed send)",
                prod.MaxOffset(mqOther) == 0,
                "B maxOffset=" + prod.MaxOffset(mqOther).ToString(CultureInfo.InvariantCulture));

     // ---------------- S6 the positive leg winds up: not one message missing ----------------
     Console.WriteLine("=== S6 a push consumer receives all the positive leg's messages ===");
            var expected = new List<string> { kSingle, kB1, kB2, kAs, kAb1, kAb2, kOw };
            WaitUntil(() =>
            {
                List<string> got = sink.Snapshot();
                return expected.All(k => got.Contains(k));
            }, 40000, 500);
            List<string> received = sink.Snapshot();
            List<string> missing = expected.Where(k => !received.Contains(k)).ToList();
   Check("S6 not one of the positive leg's messages is missing from the consumption", missing.Count == 0,
                "received=" + Join(received) + " missing=" + Join(missing));
        }
        catch (Exception e)
        {
            Check("an unexpected exception occurred during the integration", false, e.ToString());
        }
        finally
        {
            foreach (Action shutdown in new Action[]
                     {
                         () => push?.Shutdown(),
                         () => nsProd?.Shutdown(),
                         () => prod?.Shutdown(),
                     })
            {
                try
                {
                    shutdown();
                }
                catch (Exception)
                {
                // a cleanup failure does not mask the main assertion
                }
            }

            foreach (string t in new[] { topic, other, wTopic })
            {
                try
                {
                    admin.DeleteTopicInBroker(brokerAddr, t);
                }
                catch (Exception e)
                {
                    Console.WriteLine("  [WARN] deleteTopic(" + t + ") failed: " + e.Message);
                }
            }

            admin.Shutdown();
        }

        return Report();
    }

    private static string Fmt(double ms) => ms.ToString("F2", CultureInfo.InvariantCulture);

    private static bool WaitRoute(DefaultMQProducer prod, string topic, int timeoutMs = 20000)
    {
        return WaitUntil(() =>
        {
            try
            {
                return prod.FetchPublishMessageQueues(topic).Count >= 4;
            }
            catch (MQClientException)
            {
                return false;
            }
        }, timeoutMs, 500);
    }

    private static int Report()
    {
        Console.WriteLine("############ PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
            + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture) + " ############");
        return _fail == 0 ? 0 : 1;
    }
}
