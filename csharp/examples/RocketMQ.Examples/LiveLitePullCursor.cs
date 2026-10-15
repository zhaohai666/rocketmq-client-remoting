// The offline unit tests (tests/RocketMQ.Client.Tests/LitePullCursorTests.cs) only prove
// "the scripted nextBeginOffset was followed"; only a real broker lets the following two
// things hold at once: that nextBeginOffset was **computed by the broker itself**, and that
// after following it you **really receive messages**.
//
// Scenarios:
//   S1 control group: pin 1 message per queue -> assign + seek(0) + `*` -> all 4 received
//      (the path has to work, and the maxOffset == 1 ruler has to be established).
//   S2 NO_MATCHED_MSG: swap the assign expression for a Tag that never matches and seek(0)
//      again. After the broker filters out the whole range by the expression, the
//      nextBeginOffset it returns **has already passed the whole range** (= maxOffset).
//      Assert that every queue's pull cursor reached maxOffset (the old implementation only
//      advanced the cursor on FOUND, so here it would stay at 0 and rescan the same range
//      every round). Zero deliveries.
//   S3 OFFSET_ILLEGAL self-healing (the decisive one): seek(maxOffset + 1000) on every
//      queue. The broker replies with the corrected value -> the cursor must go back to
//      maxOffset; then pin 1 more message per queue and all 4 must be **received**. With
//      the old implementation the cursor stayed stuck on the out-of-range value: every
//      round it received the same "out of range" correction and not a single new message was
//      visible -- after going out of range a consumer **silently** never receives messages
//      again.
//
// Usage: rmq lite-pull-cursor [namesrv]
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveLitePullCursor
{
    private const int KQueues = 4;
    private const string KNeverMatch = "TagLiteCursorNeverMatch";
    private const long KBigAhead = 1000;

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

    private static string N(long v) => v.ToString(CultureInfo.InvariantCulture);

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

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

    private static string CursorRow(DefaultLitePullConsumer c, List<MessageQueue> mqs)
    {
        var parts = new List<string>();
        foreach (MessageQueue mq in mqs)
        {
            parts.Add("q" + N(mq.QueueId) + ":" + N(c.PullCursorOf(mq)));
        }

        return string.Join(" ", parts);
    }

    private static string Join(IEnumerable<string> items) => "[" + string.Join(", ", items) + "]";

    /// <summary>Polls repeatedly until expect messages are collected (or it times out) and
    /// returns the bodies received.</summary>
    private static List<string> Drain(DefaultLitePullConsumer c, int expect, int timeoutMs)
    {
        var outBodies = new List<string>();
        long deadline = NowMs() + timeoutMs;
        while (outBodies.Count < expect && NowMs() < deadline)
        {
            foreach (MessageExt m in c.Poll(500))
            {
                outBodies.Add(Encoding.UTF8.GetString(m.Body));
            }
        }

        return outBodies;
    }

    /// <summary>Watches the buffer within a given window (used when asserting "there must
    /// be no delivery").</summary>
    private static List<string> PollQuiet(DefaultLitePullConsumer c, int windowMs)
    {
        var outBodies = new List<string>();
        long deadline = NowMs() + windowMs;
        while (NowMs() < deadline)
        {
            foreach (MessageExt m in c.Poll(200))
            {
                outBodies.Add(Encoding.UTF8.GetString(m.Body));
            }
        }

        return outBodies;
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        string stamp = (NowMs() % 1000000).ToString(CultureInfo.InvariantCulture);
        string topic = "LiteCursorNet_" + stamp;
        string group = "GID_LiteCursorNet_" + stamp;
        Console.WriteLine("namesrv=" + namesrv + " topic=" + topic + " group=" + group);

        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(namesrv);
        admin.SetTimeoutMillis(10000);
        admin.Start();

        var prod = new DefaultMQProducer("LiteCursorNet_pg_" + stamp)
        {
            NamesrvAddr = namesrv,
            InstanceName = "lite-cursor-net-prod-" + stamp,
        };
        DefaultLitePullConsumer? c = null;
        try
        {
            admin.CreateTopic(MixAll.DefaultTopic, topic, KQueues);
            Thread.Sleep(3000);

            List<MessageQueue> mqs = admin.ExamineTopicRoute(topic).GetAllSubscribeMessageQueue(topic);
            Check("the route is visible: 4 queues", mqs.Count == KQueues, "got=" + N(mqs.Count));
            if (mqs.Count != KQueues)
            {
                return Report();
            }

            prod.Start();

            // ---------------- S1 the control group ----------------
            for (int i = 0; i < mqs.Count; ++i)
            {
                var msg = new Message(topic, Encoding.UTF8.GetBytes("lc-s1-" + N(i)))
                {
                    Tags = "TagA",
                };
                prod.Send(msg, mqs[i]);
            }

            c = new DefaultLitePullConsumer(group);
            c.SetNamesrvAddr(namesrv);
            c.SetInstanceName("lite-cursor-net-" + stamp);
            c.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
            // The out-of-range leg must never commit the out-of-range value: turn off auto
            // commit and only watch the pull/delivery cursors.
            c.SetAutoCommit(false);
            c.SetPullIntervalMillis(200);
            c.Assign(mqs);
            c.Start();
            Thread.Sleep(1000);
            foreach (MessageQueue mq in mqs)
            {
                c.Seek(mq, 0);
            }

            List<string> got1 = Drain(c, KQueues, 30000);
            Check("S1-control group: all 4 messages pinned to the queues are received under the * subscription", got1.Count == KQueues,
                "got=" + N(got1.Count) + " bodies=" + Join(got1));

            var maxes = new List<long>();
            bool maxOk = WaitUntil(() =>
            {
                maxes.Clear();
                foreach (MessageQueue mq in mqs)
                {
                    maxes.Add(admin.MaxOffset(mq));
                }

                foreach (long m in maxes)
                {
                    if (m != 1) return false;
                }

                return true;
            }, 10000);
            Check("S1-every queue has maxOffset == 1 (the ruler for the two later legs)", maxOk,
                string.Join(" ", maxes.Select(N)) + " ");

            // ---------------- S2 NO_MATCHED_MSG ----------------
            c.SetSubExpressionForAssign(topic, KNeverMatch);
            foreach (MessageQueue mq in mqs)
            {
                c.Seek(mq, 0);
            }

            bool s2 = WaitUntil(() =>
            {
                foreach (MessageQueue mq in mqs)
                {
                    if (c.PullCursorOf(mq) != 1) return false;
                }

                return true;
            }, 20000);
            Check("S2-after NO_MATCHED_MSG the pull cursor passes the whole non-matching range (== maxOffset=1)", s2,
                CursorRow(c, mqs));
            List<string> quiet = PollQuiet(c, 1000);
            Check("S2-zero deliveries during the empty responses", quiet.Count == 0, Join(quiet));

            // ---------------- S3 OFFSET_ILLEGAL ----------------
            var ahead = new List<string>();
            foreach (MessageQueue mq in mqs)
            {
                long maxOff = admin.MaxOffset(mq);
                ahead.Add(N(maxOff));
                c.Seek(mq, maxOff + KBigAhead);
            }

            bool s3 = WaitUntil(() =>
            {
                foreach (MessageQueue mq in mqs)
                {
                    if (c.PullCursorOf(mq) != 1) return false;
                }

                return true;
            }, 20000);
            Check("S3-after the broker corrects the out-of-range offset the cursor goes back to maxOffset (out of range self-heals)", s3,
                CursorRow(c, mqs) + " seekedTo=" + string.Join(" ", ahead));

            // The never-matching expression S2 switched on has to be switched back, otherwise
            // the 4 TagA messages below would be filtered out by the broker as-is.
            c.SetSubExpressionForAssign(topic, "*");
            for (int i = 0; i < mqs.Count; ++i)
            {
                var msg = new Message(topic, Encoding.UTF8.GetBytes("lc-s3-" + N(i)))
                {
                    Tags = "TagA",
                };
                prod.Send(msg, mqs[i]);
            }

            List<string> got3 = Drain(c, KQueues, 40000);
            Check("S3-after the self-healing all the new messages arrive (with the old implementation: the cursor stuck at +1000, nothing visible)",
                got3.Count == KQueues, "got=" + N(got3.Count) + " bodies=" + Join(got3));
            bool bodiesOk = got3.Count == KQueues;
            for (int i = 0; bodiesOk && i < KQueues; ++i)
            {
                bodiesOk = got3.Contains("lc-s3-" + N(i));
            }

            Check("S3-what arrived is exactly the 4 messages pinned after going out of range", bodiesOk, Join(got3));
        }
        catch (Exception e)
        {
            Check("the scenario threw", false, e.Message);
        }
        finally
        {
            if (c is not null)
            {
                try
                {
                    c.Shutdown();
                }
                catch (Exception)
                {
                    // a cleanup failure does not mask the main assertion
                }
            }

            try
            {
                prod.Shutdown();
            }
            catch (Exception)
            {
            }

            try
            {
                admin.DeleteTopic(topic);
            }
            catch (Exception e)
            {
                Console.WriteLine("  [WARN] deleteTopic(" + topic + ") failed: " + e.Message);
            }

            admin.Shutdown();
        }

        return Report();
    }

    private static int Report()
    {
        Console.WriteLine();
        Console.WriteLine("LitePullCursor live: PASS=" + _pass + " FAIL=" + _fail);
        return _fail == 0 ? 0 : 1;
    }
}
