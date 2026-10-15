// Real-machine verification of Request-Reply (5.x): initiating, the broker pushing back and
// the waiting slot being hit, the whole chain.
// Usage: rmq rr [namesrv]
//
// The chain (the scenarios and the assertions all land on protocol fields):
//   the requesting side producer.Request(msg, timeout)            the replying side push consumer
//   ──────────────────────────────                  ─────────────────────
//   CORRELATION_ID = uuid
//   REPLY_TO_CLIENT = clientId ---------->   receives the request (the broker has written CLUSTER)
//   TTL = timeout                                  create_reply_message(req, body)
//                                                    → topic = <CLUSTER>_REPLY_TOPIC
//                                                    → MSG_TYPE = "reply"
//   ◄── PUSH_REPLY_MESSAGE_TO_CLIENT(326) ────        producer.send(reply) → 325
//        the broker finds the requester's connection by REPLY_TO_CLIENT and pushes back
//
//
// Two things a real machine always trips over:
//   1) <cluster>_REPLY_TOPIC is a **system topic** the broker registers on startup; a client
//      createTopic is refused with INVALID_PARAMETER(29) "conflict with system topic", and the
//      broker side logs nothing so you can only tell from the client exception -- so
//  "the error propagation is accurate" is verified as well while at it.
//   2) consumers always use CONSUME_FROM_FIRST_OFFSET (the hard requirement in item 8 of the
//      project's MEMORY).
using System.Text;
using System.Threading;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveRR
{
    private static readonly long WallNowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static readonly string Stamp = WallNowMs.ToString(CultureInfo.InvariantCulture);
    private static readonly string Topic = "RequestReplyNet_" + Stamp;
    private static readonly string RequestGroup = "PG_RRReqNet_" + Stamp;
    private static readonly string ConsumerGroup = "CG_RRReplyNet_" + Stamp;
    private static readonly string WatchGroup = "CG_RRWatchNet_" + Stamp;

    private const int QueueNum = 4;
    // What is written in broker.conf is brokerClusterName=DefaultCluster (see /tmp/run_*_live.sh)
    private const string Cluster = "DefaultCluster";
    private static readonly string ReplyTopic = MixAll.GetReplyTopic(Cluster);
    private const int RequestTimeout = 8000;

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

    private static byte[] Str2Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static string BodyOf(MessageExt m) => Encoding.UTF8.GetString(m.Body);

    private static string BodyOf(Message m) => Encoding.UTF8.GetString(m.Body);

    // Follows the inner chain to find the broker's original exception (CreateTopic wraps the
  // MQBrokerException in an MQClientException).
    private static MQBrokerException? FindBrokerException(Exception? e)
    {
        while (e is not null)
        {
            if (e is MQBrokerException be)
            {
                return be;
            }

            e = e.InnerException;
        }

        return null;
    }

    private static bool WaitFor(Func<bool> pred, long timeoutMs)
    {
        long deadline = NowMs() + timeoutMs;
        while (NowMs() < deadline)
        {
            if (pred())
            {
                return true;
            }

            Thread.Sleep(200);
        }

        return false;
    }

    // ---------------- the replier's listener (an ordinary push consumer + a producer that sends the reply) ----------------
    private sealed class ReplierListener : IMessageListenerConcurrently
    {
        private readonly DefaultMQProducer _producer;
        private readonly object _lock = new();
        private readonly List<string> _received = new();
        private readonly List<string> _replied = new();
        private readonly List<string> _replyErrors = new();
        private bool _gotFirst;
        private string? _firstCorr;
        private string? _firstReplyTo;
        private string? _firstTtl;
        private MessageExt? _deliveredPing;

      /// <summary>The verbatim "ping-1" request that was delivered (S8 uses it to verify
    /// the CLUSTER the broker wrote).</summary>
        public MessageExt? DeliveredPing
        {
            get
            {
                lock (_lock)
                {
                    return _deliveredPing;
                }
            }
        }

        public ReplierListener(DefaultMQProducer producer) => _producer = producer;

        public DefaultMQProducer Producer => _producer;

        public int ReceivedCount
        {
            get
            {
                lock (_lock)
                {
                    return _received.Count;
                }
            }
        }

        public int RepliedCount
        {
            get
            {
                lock (_lock)
                {
                    return _replied.Count;
                }
            }
        }

        public string ReplyErrors
        {
            get
            {
                lock (_lock)
                {
                    return string.Join("; ", _replyErrors);
                }
            }
        }

        public (string? Corr, string? ReplyTo, string? Ttl) FirstRequest
        {
            get
            {
                lock (_lock)
                {
                    return (_firstCorr, _firstReplyTo, _firstTtl);
                }
            }
        }

        public bool ReceivedBody(string body)
        {
            lock (_lock)
            {
                return _received.Contains(body);
            }
        }

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
        {
            foreach (MessageExt m in msgs)
            {
                string body = BodyOf(m);
                lock (_lock)
                {
                    _received.Add(body);
                    if (body == "ping-1")
                    {
       // S8 must use the **delivered** original message to verify the CLUSTER was filled in by the broker
                        _deliveredPing = m;
                    }

                    if (!_gotFirst)
                    {
                        _gotFirst = true;
                        _firstCorr = m.GetProperty(MessageConst.PropertyCorrelationId);
                        _firstReplyTo = m.GetProperty(MessageConst.PropertyReplyToClient);
                        _firstTtl = m.GetProperty(MessageConst.PropertyMessageTTL);
                    }
                }

                try
                {
                    Message reply = RequestReply.CreateReplyMessage(m, Str2Bytes("reply:" + body));
                    _producer.Send(reply);
                    lock (_lock)
                    {
                        _replied.Add(body);
                    }
                }
                catch (Exception e)
                {
                    lock (_lock)
                    {
                        _replyErrors.Add(e.GetType().Name + ": " + e.Message);
                    }
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    // ---------------- an independent observer subscribing to <cluster>_REPLY_TOPIC (the hard evidence for S4) ----------------
    private sealed class CollectListener : IMessageListenerConcurrently
    {
        private readonly List<string> _got = new();
        private readonly object _lock = new();

        public IReadOnlyList<string> Got
        {
            get
            {
                lock (_lock)
                {
                    return _got.ToArray();
                }
            }
        }

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
        {
            lock (_lock)
            {
                foreach (MessageExt m in msgs)
                {
                    _got.Add(BodyOf(m));
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("Request-Reply live (C#): namesrv=" + namesrv + " topic=" + Topic);
     Console.WriteLine("   reply topic = " + ReplyTopic + "  cluster=" + Cluster);
        Console.WriteLine(new string('=', 70));

     // ---------------- S1 create the request topic; the reply topic is pre-registered by the broker (a client cannot create it) ----------------
        Console.WriteLine();
    Console.WriteLine("S1 create the request topic; the reply topic is pre-registered by the broker (a client cannot create it)");
        {
            var prep = new DefaultMQProducer("PG_RRPrepNet_" + Stamp)
            {
                NamesrvAddr = namesrv,
            };
            prep.Start();
            try
            {
                prep.CreateTopic("TBW102", Topic, QueueNum);
                try
                {
                    prep.CreateTopic("TBW102", ReplyTopic, QueueNum);
          Check("S1 the client's CreateTopic(reply topic) was rejected by the broker", false, "it unexpectedly succeeded");
                }
                catch (Exception e)
                {
                    MQBrokerException? be = FindBrokerException(e);
                    int code = be?.ResponseCode ?? -1;
                    string desc = be?.ResponseMessage ?? string.Empty;
   // CreateTopicInRoute embeds the MQBrokerException's message text into the outer
    // MQClientException (it does not pass the original exception through as InnerException),
    // so the text is looked at directly too. The broker's text for refusing a system topic
 // is fixed: "CODE: 29 ... conflict with system topic".
                    string full = e.Message;
                    bool ok = (code == ResponseCode.InvalidParameter
                                  && desc.Contains("system topic", StringComparison.Ordinal))
                              || (full.Contains("CODE: 29", StringComparison.Ordinal)
                                  && full.Contains("system topic", StringComparison.Ordinal));
           Check("S1 the client's CreateTopic(reply topic) was rejected (a broker system topic)",
                        ok, "code=" + (code >= 0 ? code.ToString(CultureInfo.InvariantCulture) : "?")
                        + " msg=" + full);
                }
            }
            finally
            {
                prep.Shutdown();
            }
        }

        var requester = new DefaultMQProducer(RequestGroup)
        {
            NamesrvAddr = namesrv,
            InstanceName = "RRReqNet",
        };
        requester.Start();
        // The requesting side must be registered with the broker (heartbeat) before
  // REPLY_TO_CLIENT can look the channel up, otherwise the broker's ReplyMessageProcessor
        // finds no channel and replies SYSTEM_ERROR to the replier.
  // C# issues one heartbeat inside Request() (SendHeartbeatToAllBroker); here it only
 // confirms the instance is up and the clientId has been generated (the real proof of
  // registration is that S3 gets a response).
    Check("S1 the requesting side is up and its clientId has been generated",
            requester.IsStarted && requester.ClientId.Length > 0, "clientId=" + requester.ClientId);

        var replierProducer = new DefaultMQProducer("PG_RRReplierNet_" + Stamp)
        {
            NamesrvAddr = namesrv,
            InstanceName = "RRReplierNet",
        };
        replierProducer.Start();
        var replier = new ReplierListener(replierProducer);
        var replierConsumer = new DefaultMQPushConsumer(ConsumerGroup)
        {
            InstanceName = "RRConsumerNet",
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        replierConsumer.SetNamesrvAddr(namesrv);
        replierConsumer.Subscribe(Topic, "*");
        replierConsumer.SetMessageListener(replier);
        replierConsumer.Start();

     // The observer: independently subscribes to <cluster>_REPLY_TOPIC, the hard evidence for S4
        var watcher = new CollectListener();
        var watchConsumer = new DefaultMQPushConsumer(WatchGroup)
        {
            InstanceName = "RRWatchNet",
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        watchConsumer.SetNamesrvAddr(namesrv);
        watchConsumer.Subscribe(ReplyTopic, "*");
        watchConsumer.SetMessageListener(watcher);
        watchConsumer.Start();
   Check("S1 the reply consumer / observer started",
            replierConsumer.IsStarted && watchConsumer.IsStarted, "");

        try
        {
            // ---------------- S2/S3 one complete round trip ----------------
            Console.WriteLine();
    Console.WriteLine("S2/S3 one complete request -> reply round trip");
            var msg = new Message(Topic, Str2Bytes("ping-1"))
            {
                Keys = "rr",
            };
            Message reply = requester.Request(msg, RequestTimeout);

            string want = "reply:ping-1";
     Check("S3 Request() got the response and the body is correct", BodyOf(reply) == want,
                "got=" + BodyOf(reply) + " want=" + want);
    Check("S3 the reply topic is <cluster>_REPLY_TOPIC", reply.Topic == ReplyTopic,
                "topic=" + reply.Topic);
            string? arrive = reply.GetProperty(MessageConst.PropertyReplyMessageArriveTime);
  Check("S3 the reply carries REPLY_MESSAGE_ARRIVE_TIME (the stamp the client puts on when it receives it)",
                !string.IsNullOrEmpty(arrive) && arrive.All(char.IsDigit), "value=" + arrive);

            bool gotReq = WaitFor(() => replier.ReceivedCount >= 1, 15000);
     Check("S2 the replier received the request message", gotReq, "received=" + replier.ReceivedCount);
            if (gotReq)
            {
                (string? corr, string? replyTo, string? ttl) = replier.FirstRequest;
             Check("S2 the request message carries CORRELATION_ID (a uuid string)",
                    !string.IsNullOrEmpty(corr) && corr.Length == 36, "corr=" + corr);
      Check("S2 the request message carries REPLY_TO_CLIENT = the requester's clientId",
                    replyTo == requester.ClientId, "replyTo=" + replyTo
                    + " clientId=" + requester.ClientId);
  Check("S2 the request message carries TTL = timeout", ttl == RequestTimeout.ToString(CultureInfo.InvariantCulture),
                    "ttl=" + ttl);
            }

          // ---------------- S4 the reply really went through 325 and landed on REPLY_TOPIC ----------------
            Console.WriteLine();
    Console.WriteLine("S4 the reply goes out via SEND_REPLY_MESSAGE_V2(325) and lands on <cluster>_REPLY_TOPIC");
            bool replied = WaitFor(() => replier.RepliedCount >= 1, 15000);
      Check("S4 the replier sent the reply message successfully", replied,
                "replied=" + replier.RepliedCount + " errors=" + replier.ReplyErrors);
            bool seen = WaitFor(() => watcher.Got.Contains(want), 15000);
            Check("S4 the independent consumer of " + ReplyTopic + " can see that reply",
                seen, "watched=[" + string.Join(" ", watcher.Got) + "]");

  // ---------------- S5 no replier -> RequestTimeoutException ----------------
            Console.WriteLine();
  Console.WriteLine("S5 with no replier, Request() throws RequestTimeoutException");
            string silentTopic = Topic + "_NoReplier";
            {
                var prep2 = new DefaultMQProducer("PG_RRPrep2Net_" + Stamp)
                {
                    NamesrvAddr = namesrv,
                    InstanceName = "RRPrep2Net",
                };
                try
                {
                    prep2.Start();
                    prep2.CreateTopic("TBW102", silentTopic, QueueNum);
                }
                finally
                {
                    prep2.Shutdown();
                }
            }

            long t0 = NowMs();
            Exception? raised = null;
            try
            {
                var silent = new Message(silentTopic, Str2Bytes("nobody-home"))
                {
                    Keys = "rr-silent",
                };
                requester.Request(silent, 3000);
            }
            catch (Exception e)
            {
                raised = e;
            }

            long elapsed = NowMs() - t0;
         Check("S5 what is thrown is a RequestTimeoutException (not a silent return / some other exception)",
                raised is RequestTimeoutException,
                "raised=" + (raised?.GetType().Name ?? "null") + ": " + raised?.Message);
    Check("S5 the exception carries 10006 REQUEST_TIMEOUT_EXCEPTION",
                raised is MQClientException mce
                && mce.ResponseCode == ClientErrorCode.RequestTimeoutException,
                "code=" + (raised as MQClientException)?.ResponseCode);
     Check("S5 the timeout duration is close to the configured value (2s ~ 12s, showing it really waited rather than failing at once)",
                2000 <= elapsed && elapsed <= 12000, "elapsed=" + elapsed.ToString(CultureInfo.InvariantCulture) + "ms");

          // ---------------- S6 concurrent requests do not cross wires ----------------
            Console.WriteLine();
     Console.WriteLine("S6 3 concurrent requests, each getting its own reply");
            var results = new System.Collections.Concurrent.ConcurrentDictionary<int, string>();
            var errors = new List<string>();
            var tasks = new List<Task>();
            for (int i = 0; i < 3; ++i)
            {
                int local = i;
                tasks.Add(Task.Run(() =>
                {
                    try
                    {
                        var m = new Message(Topic, Str2Bytes("ping-conc-" + local.ToString(CultureInfo.InvariantCulture)))
                        {
                            Keys = "rr-conc",
                        };
                        Message r = requester.Request(m, RequestTimeout);
                        results[local] = BodyOf(r);
                    }
                    catch (Exception e)
                    {
                        lock (errors)
                        {
                            errors.Add("i=" + local.ToString(CultureInfo.InvariantCulture)
                                + " " + e.GetType().Name + ": " + e.Message);
                        }
                    }
                }));
            }

            Task.WaitAll(tasks.ToArray(), RequestTimeout + 5000);
            bool s6Ok = results.Count == 3 && errors.Count == 0;
            for (int i = 0; i < 3; ++i)
            {
                string expect = "reply:ping-conc-" + i.ToString(CultureInfo.InvariantCulture);
                if (!results.TryGetValue(i, out string? got) || got != expect)
                {
                    s6Ok = false;
                }
            }

            Check("S6 all 3 concurrent requests got a reply and they match one to one",
                s6Ok, "results=" + string.Join(",", results.OrderBy(k => k.Key)
                    .Select(k => k.Key.ToString(CultureInfo.InvariantCulture) + "=>" + k.Value))
                + " errors=" + string.Join("; ", errors));

     // ---------------- S7 the consumer is not disturbed by the Reply traffic ----------------
            Console.WriteLine();
        Console.WriteLine("S7 the replier's consumer is still intact (ordinary push consumption is unaffected by Reply)");
            var plain = new Message(Topic, Str2Bytes("plain-after-replies"))
            {
                Keys = "rr-plain",
            };
            requester.Send(plain);
            bool plainOk = WaitFor(() => replier.ReceivedBody("plain-after-replies"), 15000);
     Check("S7 subsequent ordinary messages are still consumed", plainOk,
                "received=" + replier.ReceivedCount);

            Console.WriteLine();
     Console.WriteLine("S8 CLUSTER is written by the broker; when the reply cannot be built it reports 10007 rather than some other error");
            MessageExt? delivered = replier.DeliveredPing;
     Check("S8 the delivered request message carries the broker-written CLUSTER=" + Cluster,
                delivered?.GetProperty(MessageConst.PropertyCluster) == Cluster,
                "cluster=" + delivered?.GetProperty(MessageConst.PropertyCluster));
            // GetProperty returns an empty string (not null) for a key that is not there, so
  // this checks "empty" rather than "null"; the 10007 assertion below is the hard evidence
      // that "this really has no CLUSTER".
            Check("S8 the request message on the client side has **no** CLUSTER (the property really was filled in by the broker)",
                msg.GetProperty(MessageConst.PropertyCluster).Length == 0,
                "value=\"" + msg.GetProperty(MessageConst.PropertyCluster) + "\"");

            Exception? raised8 = null;
            try
            {
                RequestReply.CreateReplyMessage(msg, Str2Bytes("pong"));
            }
            catch (Exception e)
            {
                raised8 = e;
            }

      Check("S8 building a reply from the local copy of the request message -> MQClientException carrying 10007",
                raised8 is MQClientException c8
                && c8.ResponseCode == ClientErrorCode.CreateReplyMessageException,
                "raised=" + (raised8?.GetType().Name ?? "null") + " code="
                + (raised8 as MQClientException)?.ResponseCode);
   Check("S8 the 10007 message is verbatim as agreed by the protocol and points at the missing CLUSTER property",
                raised8?.Message.Contains("property[" + MessageConst.PropertyCluster
                                          + "] is null.") == true,
                "msg=" + raised8?.Message);

            Exception? raised8b = null;
            try
            {
                RequestReply.CreateReplyMessage(null!, Str2Bytes("pong"));
            }
            catch (Exception e)
            {
                raised8b = e;
            }

            Check("S8 a null request message -> also 10007 (not a bare NullReferenceException)",
                raised8b is MQClientException c8b
                && c8b.ResponseCode == ClientErrorCode.CreateReplyMessageException,
                "raised=" + (raised8b?.GetType().Name ?? "null") + " code="
                + (raised8b as MQClientException)?.ResponseCode);
        }
        finally
        {
            requester.Shutdown();
            replierConsumer.Shutdown();
            watchConsumer.Shutdown();
        }

        Console.WriteLine();
        Console.WriteLine("Request-Reply: PASS=" + _pass + " FAIL=" + _fail);
        return _fail == 0 ? 0 : 1;
    }
}
