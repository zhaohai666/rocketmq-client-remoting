// Real-machine verification of the active pull consumer (DefaultMQPullConsumer).
// Usage: rmq pull [namesrv]
//
// Scenarios (the core of pull mode is "the caller pulls and manages the offsets itself", and
// every assertion circles around that):
//   S1 create the topic + FetchSubscribeMessageQueues -> get 4 queues
//   S1b FetchMessageQueuesInBalance -> an instance that never pulled computes its own share
//       from the real route + 38
//   S2 produce 12 messages -> the min/max offset difference per queue = 3 (the messages spread
//      evenly over 4 queues)
//   S3 manual pull: per queue from the min offset to the max offset -> all 12 received and
//      the bodies match the sent set
//   S4 manual offset commit: UpdateConsumeOffset -> reading it back with FetchConsumeOffset
//      agrees (a broker round trip)
//   S5 the offsets are the caller's to control: pulling again from the committed offset ->
//       NoNewMsg; rolling the offset back to min and pulling again -> Found (push mode cannot
//       do this, which is precisely why pull mode exists)
//   S6 SearchOffset(now) / EarliestMsgStoreTime -> both > 0
//   S7 SendMessageBack -> the message lands in %RETRY%group and can be pulled (the resend
//      path really works)
//
// Two pitfalls already hit (do not "tidy them away" by hand):
//   1. The topic must be **created before the queues are fetched**: the consumer does no
//      default-topic fallback, so a non-existent topic yields no route;
//   2. maxOffset must **not** be queried right after producing: the broker's consumequeue is
//      dispatched asynchronously so you read 0 (measured with a probe), and you have to poll
//      until the sum of max-min across the queues is in place.
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;

namespace RocketMQ.Examples;

public static class LivePull
{
    private static readonly long WallNowMs = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static readonly string Stamp = WallNowMs.ToString(CultureInfo.InvariantCulture);
    private static readonly string Topic = "PullLiveNet_" + Stamp;
    private static readonly string Group = "PG_PullLiveNet_" + Stamp;
    private static readonly string RetryTopic = MixAll.GetRetryTopic(Group);

    private const int NMsg = 12;
    private const int QueueNum = 4;
    private const int PerQueue = NMsg / QueueNum;

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

    private static string QKey(MessageQueue q) =>
        q.BrokerName + ":" + q.QueueId.ToString(CultureInfo.InvariantCulture);

    private static string Join(IEnumerable<string> items) => string.Join(" ", items);

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("PullConsumer live (C#): namesrv=" + namesrv + " topic=" + Topic
            + " group=" + Group);
        Console.WriteLine(new string('=', 70));

        // ---------------- create the topic ----------------
        {
            var prep = new DefaultMQProducer("PG_PrepareNet_" + Stamp);
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

        var consumer = new DefaultMQPullConsumer(Group);
        consumer.SetNamesrvAddr(namesrv);

        // ---------------- S1 the queues ----------------
        Console.WriteLine();
        Console.WriteLine("S1 create the topic + FetchSubscribeMessageQueues");
        List<MessageQueue> routes;
        {
            consumer.Start();
            routes = new List<MessageQueue>();
            long deadline = NowMs() + 20000;
            string last = "not tried";
            while (NowMs() < deadline)
            {
                try
                {
                    routes = consumer.FetchSubscribeMessageQueues(Topic);
                    if (routes.Count >= QueueNum)
                    {
                        break;
                    }

                    last = routes.Count.ToString(CultureInfo.InvariantCulture) + " queues";
                }
                catch (Exception e)
                {
                    last = e.GetType().Name + ": " + e.Message;
                }

                Thread.Sleep(500);
            }

            if (routes.Count != QueueNum)
            {
       Check("S1 got " + QueueNum.ToString(CultureInfo.InvariantCulture) + " queues", false,
                    "got=" + routes.Count.ToString(CultureInfo.InvariantCulture) + " last=" + last);
                Console.WriteLine();
                Console.WriteLine("PullConsumer: PASS=" + _pass + " FAIL=" + _fail);
                consumer.Shutdown();
                return 1;
            }
        }

        Check("S1 got " + QueueNum.ToString(CultureInfo.InvariantCulture) + " queues", true,
            "got=" + routes.Count.ToString(CultureInfo.InvariantCulture));
        {
            bool namesOk = routes.All(q => q.Topic == Topic && q.BrokerName.Length > 0);
       Check("S1 the queue's topic and broker name are non-empty", namesOk, "sample=" + QKey(routes[0]));
        }

        Console.WriteLine();
        Console.WriteLine("S1b FetchMessageQueuesInBalance");
        {
            consumer.RegisterMessageQueueListener(Topic, null);
            int hbOk = consumer.SendHeartbeatToAllBroker();
          Check("S1b after registering the topic the heartbeat went out (the precondition for being visible to 38)", hbOk > 0,
                "heartbeat_ok=" + hbOk.ToString(CultureInfo.InvariantCulture));

            var mine = new List<MessageQueue>();
            long balanceDeadline = NowMs() + 20000;
            while (NowMs() < balanceDeadline)
            {
                try
                {
                    mine = consumer.FetchMessageQueuesInBalance(Topic);
                    if (mine.Count == QueueNum)
                    {
                        break;
                    }
                }
                catch (Exception e)
                {
                    Console.WriteLine("  [diag] FetchMessageQueuesInBalance: " + e.Message);
                }

                consumer.SendHeartbeatToAllBroker();
                Thread.Sleep(500);
            }

            Check("S1b the balance view: an exclusive group gets all "
             + QueueNum.ToString(CultureInfo.InvariantCulture) + " queues"
      + " (never having pulled => it can only be route+38 computed)",
                mine.Count == QueueNum,
                "got=" + mine.Count.ToString(CultureInfo.InvariantCulture) + " of "
                + QueueNum.ToString(CultureInfo.InvariantCulture));
            var whole = new HashSet<string>(routes.Select(QKey), StringComparer.Ordinal);
    Check("S1b the balance view is a subset of the subscription view", mine.All(q => whole.Contains(QKey(q))),
                "mine=" + string.Join(" ", mine.Select(QKey)));
        }

    // ---------------- S2 produce ----------------
        Console.WriteLine();
        Console.WriteLine("S2 producing " + NMsg.ToString(CultureInfo.InvariantCulture) + " messages");
        var sent = new HashSet<string>(StringComparer.Ordinal);
        {
            var prod = new DefaultMQProducer("PG_PullLiveNet_" + Stamp);
            prod.NamesrvAddr = namesrv;
            prod.Start();
            int sentOk = 0;
            for (int i = 0; i < NMsg; ++i)
            {
                string body = "pull-" + i.ToString("D2", CultureInfo.InvariantCulture);
                try
                {
                    var msg = new Message(Topic, Str2Bytes(body));
                    msg.PutProperty("KEYS", "pull-key-" + body);
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

  Check("S2 producing " + NMsg.ToString(CultureInfo.InvariantCulture) + " messages succeeded", sentOk == NMsg,
                "sentOk=" + sentOk.ToString(CultureInfo.InvariantCulture));
            prod.Shutdown();
        }

        // poll until the consumequeue dispatch lands (you cannot query maxOffset right after sending)
        var lo = new Dictionary<string, long>(StringComparer.Ordinal);
        var hi = new Dictionary<string, long>(StringComparer.Ordinal);
        {
            long deadline = NowMs() + 25000;
            while (NowMs() < deadline)
            {
                lo.Clear();
                hi.Clear();
                long total = 0;
                foreach (MessageQueue q in routes)
                {
                    lo[QKey(q)] = consumer.MinOffset(q);
                    hi[QKey(q)] = consumer.MaxOffset(q);
                    total += Math.Max(0, hi[QKey(q)] - lo[QKey(q)]);
                }

                if (total >= NMsg)
                {
                    break;
                }

                Thread.Sleep(500);
            }
        }

        foreach (MessageQueue q in routes)
        {
            long diff = hi[QKey(q)] - lo[QKey(q)];
   Check("S2 the queue " + QKey(q) + " has " + PerQueue.ToString(CultureInfo.InvariantCulture),
                diff == PerQueue,
                "min=" + lo[QKey(q)].ToString(CultureInfo.InvariantCulture)
                    + " max=" + hi[QKey(q)].ToString(CultureInfo.InvariantCulture));
        }

     // ---------------- S3 the manual pull ----------------
        Console.WriteLine();
        Console.WriteLine("S3 manual pull (per queue min -> max)");
        var got = new HashSet<string>(StringComparer.Ordinal);
        foreach (MessageQueue q in routes)
        {
            long offset = lo[QKey(q)];
            long end = hi[QKey(q)];
            int guard = 0;
            while (offset < end && guard < 64)
            {
                ++guard;
                PullResult r;
                try
                {
                    r = consumer.Pull(q, "*", offset, 32, 5000);
                }
                catch (Exception e)
                {
          Check("S3 pulling " + QKey(q) + " threw", false, e.Message);
                    break;
                }

                if (r.Status == PullStatus.Found)
                {
                    foreach (MessageExt m in r.MsgFoundList)
                    {
                        got.Add(BodyOf(m));
                    }

                    if (r.NextBeginOffset <= offset)
                    {
                        break;
                    }

                    offset = r.NextBeginOffset;
                }
                else if (r.Status == PullStatus.NoNewMsg)
                {
                    break;
                }
                else if (r.Status == PullStatus.OffsetIllegal)
                {
                    break;
                }
                else
                {
           Check("S3 pulling " + QKey(q) + " came back with an unexpected status", false, "status=" + r.Status);
                    break;
                }
            }
        }

        {
            var missing = sent.Except(got, StringComparer.Ordinal).OrderBy(x => x, StringComparer.Ordinal);
            var extra = got.Except(sent, StringComparer.Ordinal).OrderBy(x => x, StringComparer.Ordinal);
      Check("S3 the manual pull received all " + NMsg.ToString(CultureInfo.InvariantCulture) + " messages with matching content",
                got.SetEquals(sent),
                "got=" + got.Count.ToString(CultureInfo.InvariantCulture) + " missing=[" + Join(missing)
                    + "] extra=[" + Join(extra) + "]");
        }

        if (got.Count == 0)
        {
            Console.WriteLine();
     Console.WriteLine("!! nothing was pulled at all, the later scenarios are skipped");
            Console.WriteLine();
            Console.WriteLine("PullConsumer: PASS=" + _pass + " FAIL=" + _fail);
            consumer.Shutdown();
            return 1;
        }

     // ---------------- S4 the manual offset commit ----------------
        Console.WriteLine();
        Console.WriteLine("S4 committing the offset by hand and reading it back");
        {
            MessageQueue q0 = routes[0];
            long target = hi[QKey(q0)];
            consumer.UpdateConsumeOffset(q0, target);
            bool haveOffset = consumer.FetchConsumeOffset(q0, out long back);
        Check("S4 the offset reads back the same after the commit", haveOffset && back == target,
                "committed=" + back.ToString(CultureInfo.InvariantCulture)
                    + " target=" + target.ToString(CultureInfo.InvariantCulture));
        }

     // ---------------- S5 the offsets are the caller's to control ----------------
        Console.WriteLine();
        Console.WriteLine("S5 the offsets are the caller's to control");
        {
            MessageQueue q0 = routes[0];
            long committed = hi[QKey(q0)];
            PullResult r1 = consumer.Pull(q0, "*", committed, 32, 5000);
          Check("S5 pulling again from the committed offset = NoNewMsg",
                r1.Status == PullStatus.NoNewMsg && r1.MsgFoundList.Count == 0,
                "status=" + r1.Status + " n="
                    + r1.MsgFoundList.Count.ToString(CultureInfo.InvariantCulture));

            PullResult r2 = consumer.Pull(q0, "*", lo[QKey(q0)], 32, 5000);
   Check("S5 after rolling the offset back to min it can be pulled again (the core capability of pull mode)",
                r2.Status == PullStatus.Found && r2.MsgFoundList.Count > 0,
                "status=" + r2.Status + " n="
                    + r2.MsgFoundList.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------------- S6 offset queries ----------------
        Console.WriteLine();
        Console.WriteLine("S6 SearchOffset / EarliestMsgStoreTime / min/max");
        {
            MessageQueue q0 = routes[0];
            long so = consumer.SearchOffset(q0, NowMs());
            Check("S6 SearchOffset(now) > 0", so > 0,
                "searchOffset=" + so.ToString(CultureInfo.InvariantCulture));
            long emst = consumer.EarliestMsgStoreTime(q0);
            Check("S6 EarliestMsgStoreTime > 0", emst > 0,
                "earliest=" + emst.ToString(CultureInfo.InvariantCulture));
            Check("S6 MinOffset <= MaxOffset", lo[QKey(q0)] <= hi[QKey(q0)],
                "min=" + lo[QKey(q0)].ToString(CultureInfo.InvariantCulture)
                    + " max=" + hi[QKey(q0)].ToString(CultureInfo.InvariantCulture));
        }

        // ---------------- S7 the resend ----------------
        Console.WriteLine();
        Console.WriteLine("S7 SendMessageBack -> pullable from %RETRY%group");
        {
            MessageExt? sample = null;
            foreach (MessageQueue q in routes)
            {
                PullResult r = consumer.Pull(q, "*", lo[QKey(q)], 1, 5000);
                if (r.MsgFoundList.Count > 0)
                {
                    sample = r.MsgFoundList[0];
                    break;
                }
            }

            if (sample is null)
            {
     Check("S7 got a sample message", false, "no message available");
            }
            else
            {
                string want = BodyOf(sample);
                try
                {
                    consumer.SendMessageBack(sample, 0);
          Check("S7 the resend request was accepted by the broker", true,
                        "offset=" + sample.CommitLogOffset.ToString(CultureInfo.InvariantCulture)
                            + " body=" + want);
                }
                catch (Exception e)
                {
        Check("S7 the resend request was accepted by the broker", false, e.Message);
                    Console.WriteLine();
                    Console.WriteLine("PullConsumer: PASS=" + _pass + " FAIL=" + _fail);
                    consumer.Shutdown();
                    return _fail == 0 ? 0 : 1;
                }

                bool found = false;
                string detail = "not tried";
                long deadline = NowMs() + 40000;
                while (NowMs() < deadline && !found)
                {
                    List<MessageQueue> rqs;
                    try
                    {
                        rqs = consumer.FetchSubscribeMessageQueues(RetryTopic);
                    }
                    catch (Exception e)
                    {
                        detail = "retry topic not routable yet: " + e.Message;
                        Thread.Sleep(1000);
                        continue;
                    }

                    foreach (MessageQueue rq in rqs)
                    {
                        long rlo;
                        long rhi;
                        try
                        {
                            rlo = consumer.MinOffset(rq);
                            rhi = consumer.MaxOffset(rq);
                        }
                        catch (Exception e)
                        {
                            detail = e.Message;
                            continue;
                        }

                        if (rhi <= rlo)
                        {
                            continue;
                        }

                        PullResult r = consumer.Pull(rq, "*", rlo, 32, 5000);
                        foreach (MessageExt m in r.MsgFoundList)
                        {
                            if (BodyOf(m) == want)
                            {
                                found = true;
                                detail = "queue=" + rq.QueueId.ToString(CultureInfo.InvariantCulture)
                                    + " reconsumeTimes="
                                    + m.ReconsumeTimes.ToString(CultureInfo.InvariantCulture)
                                    + " body=" + BodyOf(m);
                                break;
                            }
                        }

                        if (found)
                        {
                            break;
                        }
                    }

                    if (!found)
                    {
                        Thread.Sleep(1000);
                    }
                }

     Check("S7 the resent message was pulled from %RETRY%", found, detail);
            }
        }

        consumer.Shutdown();

        Console.WriteLine();
        Console.WriteLine("PullConsumer: PASS=" + _pass + " FAIL=" + _fail);
        return _fail == 0 ? 0 : 1;
    }
}
