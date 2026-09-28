// lite-pull **拉取游标**（#105，Java DefaultLitePullConsumerImpl#PullTaskImpl.run:982-998）
// 真机验证。与 cpp/examples/live_lite_pull_cursor.cpp、python/verify_lite_pull_cursor_live.py、
// rust/examples/live_lite_pull_cursor.rs 同题、逐条对应。
//
// 离线单测（tests/RocketMQ.Client.Tests/LitePullCursorTests.cs）只能证明「脚本回的
// nextBeginOffset 被跟了」；真 broker 才能让下面两件事同时成立：那个 nextBeginOffset 是
// **broker 自己算的**，而且跟过去以后**真的能收到消息**。
//
// 场景：
//   S1 对照组：每条队列钉 1 条 → assign + seek(0) + `*` → 4 条全收（链路要通，
//      maxOffset == 1 这个标尺也要立住）。
//   S2 NO_MATCHED_MSG：把 assign 表达式换成永不匹配的 Tag 再 seek(0)。broker 按表达式把
//      整段滤掉后回的 nextBeginOffset **已经越过整段**（= maxOffset）。断言每条队列的拉取
//      游标都到 maxOffset（旧实现只在 FOUND 时推游标，这里会永远停在 0，每轮重扫同一段）。
//      零投递。
//   S3 OFFSET_ILLEGAL 自愈（决定性一条）：每条队列 seek(maxOffset + 1000)。broker 回纠正值
//      → 游标必须回到 maxOffset；随后每条队列再钉 1 条，4 条必须**全部收到**。旧实现的游标
//      永远卡在越界值上：每轮收到同一个「越界纠正」，新消息一条也看不到 —— 越界之后消费者
//      会**静默**地永远收不到消息。
//
// 用法：rmq lite-pull-cursor [namesrv]
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

    /// <summary>反复 poll 直到收齐 expect 条（或超时），返回收到的 body。</summary>
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

    /// <summary>给定窗口里盯住缓冲（断言「不该有交付」时用）。</summary>
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
            Check("路由可见：4 条队列", mqs.Count == KQueues, "got=" + N(mqs.Count));
            if (mqs.Count != KQueues)
            {
                return Report();
            }

            prod.Start();

            // ---------------- S1 对照组 ----------------
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
            // 位点越界那一腿绝不能把越界值提交上去：关掉自动提交，只看拉取/交付两条游标。
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
            Check("S1-对照组：* 订阅下 4 条钉到队列的消息全收", got1.Count == KQueues,
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
            Check("S1-每条队列 maxOffset == 1（后面两条腿的标尺）", maxOk,
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
            Check("S2-NO_MATCHED_MSG 后拉取游标越过整段不匹配区间（== maxOffset=1）", s2,
                CursorRow(c, mqs));
            List<string> quiet = PollQuiet(c, 1000);
            Check("S2-空应答期间零投递", quiet.Count == 0, Join(quiet));

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
            Check("S3-越界位点被 broker 纠正后游标回到 maxOffset（越界自愈）", s3,
                CursorRow(c, mqs) + " seekedTo=" + string.Join(" ", ahead));

            // S2 换上的永不匹配表达式要换回来，否则下面 4 条 TagA 会被 broker 原样滤掉。
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
            Check("S3-自愈后新消息全部送达（旧实现：游标卡在 +1000，一条都看不到）",
                got3.Count == KQueues, "got=" + N(got3.Count) + " bodies=" + Join(got3));
            bool bodiesOk = got3.Count == KQueues;
            for (int i = 0; bodiesOk && i < KQueues; ++i)
            {
                bodiesOk = got3.Contains("lc-s3-" + N(i));
            }

            Check("S3-收到的正是越界之后钉进去的那 4 条", bodiesOk, Join(got3));
        }
        catch (Exception e)
        {
            Check("场景异常", false, e.Message);
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
                    // 收尾失败不掩盖主断言
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
