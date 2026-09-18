// 轻量拉取消费者（DefaultLitePullConsumer）真机验证。
// 用法：rmq lite-pull [namesrv]
//
// 与 cpp/examples/live_lite_pull.cpp、python/verify_lite_pull_live.py 完全同场景
// （三语言对拍用同一套断言）。
//
// 场景（围绕 Lite 相对 Pull 的本质区别：调用方不用管位点，poll 从本地缓冲拿消息）：
//   S1 建 topic + subscribe 模式等待 rebalance 分到位点（4/4 队列）
//   S2 先起消费者、再发 12 条（交替 TagA/TagB）→ subscribe + poll 收全 12 条且内容一致
//   S3 auto-commit：消费后 committed 位点 > 0，且 commit() 后可回读
//   S4 assign 模式：显式 assign 全部队列 + seek 到队首 → poll 重新收全 12 条
//   S5 订阅 TagA：Subscribe(T, "TagA") 只收 TagA 的 6 条（订阅级 tag 过滤）
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

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        string group1 = "LitePG1Net_" + Stamp;
        string group2 = "LitePG2Net_" + Stamp;
        string group3 = "LitePG3Net_" + Stamp;

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("LitePullConsumer live (.NET): namesrv=" + namesrv + " topic=" + Topic
            + " group=" + group1);
        Console.WriteLine(new string('=', 70));

        // ---------------- 建 topic ----------------
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

        // ---------------- S1 subscribe 模式：先起消费者再发消息 ----------------
        Console.WriteLine();
        Console.WriteLine("S1 subscribe 模式启动 + 等待 rebalance 分到位点");
        var c1 = new DefaultLitePullConsumer(group1);
        c1.SetNamesrvAddr(namesrv);
        c1.SetPollTimeoutMillis(1000);
        c1.Subscribe(Topic, "*");
        c1.Start();

        List<MessageQueue> assigned = WaitAssignment(c1);
        Check("S1 rebalance 分配到 " + QueueNum.ToString(CultureInfo.InvariantCulture) + " 个队列",
            assigned.Count == QueueNum,
            "assigned=" + assigned.Count.ToString(CultureInfo.InvariantCulture));
        if (assigned.Count == 0)
        {
            Console.WriteLine("!! 未分配到队列，后续跳过");
            c1.Shutdown();
            Console.WriteLine();
            Console.WriteLine("LitePullConsumer: PASS=" + _pass + " FAIL=" + _fail);
            return 1;
        }

        // ---------------- S2 生产 + poll 收全 ----------------
        Console.WriteLine();
        Console.WriteLine("S2 生产 " + NMsg.ToString(CultureInfo.InvariantCulture) + " 条（交替 TagA/TagB）");
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

            Check("S2 生产 " + NMsg.ToString(CultureInfo.InvariantCulture) + " 条成功", sentOk == NMsg,
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
            Check("S2 subscribe+poll 收全 "
                + NMsg.ToString(CultureInfo.InvariantCulture) + " 条且内容一致", gotSet.SetEquals(sent),
                "got=" + gotSet.Count.ToString(CultureInfo.InvariantCulture)
                + " missing=[" + Join(missing) + "] extra=[" + Join(extra) + "]");
        }

        // ---------------- S3 auto-commit 位点 ----------------
        Console.WriteLine();
        Console.WriteLine("S3 auto-commit 位点");
        {
            bool allPositive = true;
            var detail = new StringBuilder();
            foreach (MessageQueue mq in assigned)
            {
                long v = c1.Committed(mq);
                detail.Append(v.ToString(CultureInfo.InvariantCulture)).Append(' ');
                if (v <= 0)
                {
                    allPositive = false;
                }
            }

            Check("S3 各队列 committed 位点 > 0", allPositive, "committed=" + detail);
        }

        // ---------------- S4 assign 模式：assign 全部队列 + seek 到队首重新收全 ----------------
        Console.WriteLine();
        Console.WriteLine("S4 assign 模式：assign 全部队列 + seek 到队首重新收全");
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

            Check("S4 assign+seek+poll 重新收全 "
                + NMsg.ToString(CultureInfo.InvariantCulture) + " 条", got2Set.SetEquals(sent),
                "got=" + got2Set.Count.ToString(CultureInfo.InvariantCulture));
            c2.Shutdown();
        }

        // ---------------- S5 订阅级 tag 过滤 ----------------
        Console.WriteLine();
        Console.WriteLine("S5 订阅 TagA：只收 TagA 的 6 条");
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

            // 偶数下标（lite-00/02/...）才是 TagA
            bool onlyA = got3Set.Count == 6
                && got3Set.All(b => (b[^1] - '0') % 2 == 0);
            Check("S5 仅收 TagA 且恰好 6 条", onlyA,
                "got=" + got3Set.Count.ToString(CultureInfo.InvariantCulture));
            c3.Shutdown();
        }

        c1.Shutdown();

        Console.WriteLine();
        Console.WriteLine("LitePullConsumer: PASS=" + _pass + " FAIL=" + _fail);
        return _fail == 0 ? 0 : 1;
    }
}
