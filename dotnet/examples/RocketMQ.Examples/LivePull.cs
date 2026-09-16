// 主动拉取消费者（DefaultMQPullConsumer）真机验证。
// 用法：rmq pull [namesrv]
//
// 与 cpp/examples/live_pull.cpp、python/verify_pull_live.py 完全同场景
// （三语言对拍用同一套断言）。
//
// 场景（拉模式的核心是「调用方自己拉、自己管位点」，断言都围绕这一点）：
//   S1 建 topic + FetchSubscribeMessageQueues → 拿到 4 个队列
//   S2 生产 12 条 → 每队列 min/max offset 差值 = 3（消息均匀落到 4 队列）
//   S3 手动拉取：逐队列从 min offset 拉到 max offset → 收全 12 条且 body 与发送集合一致
//   S4 手动提交位点：UpdateConsumeOffset → FetchConsumeOffset 回读一致（broker 往返）
//   S5 位点由调用方掌控：从已提交位点再拉 → NoNewMsg；把位点退回 min 再拉 → Found
//      （push 模式做不到这一点，这正是 pull 模式的存在意义）
//   S6 SearchOffset(now) / EarliestMsgStoreTime → 均 > 0
//   S7 SendMessageBack → 消息落到 %RETRY%group，可被拉取到（回投链路真实可用）
//
// ⚠ 两个踩过的坑（不要"顺手优化"掉）：
//   1. 必须**先建 topic 再取队列**：消费者不做默认 topic 兜底，topic 不存在就拿不到路由；
//   2. 生产完**不能立刻查 maxOffset**：broker 的 consumequeue 是异步分发的，会读到 0
//      （探针实测），必须轮询到各队列 max-min 之和到位。
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
        Console.WriteLine("PullConsumer live (.NET): namesrv=" + namesrv + " topic=" + Topic
            + " group=" + Group);
        Console.WriteLine(new string('=', 70));

        // ---------------- 建 topic ----------------
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

        // ---------------- S1 队列 ----------------
        Console.WriteLine();
        Console.WriteLine("S1 建 topic + FetchSubscribeMessageQueues");
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
                Check("S1 拿到 " + QueueNum.ToString(CultureInfo.InvariantCulture) + " 个队列", false,
                    "got=" + routes.Count.ToString(CultureInfo.InvariantCulture) + " last=" + last);
                Console.WriteLine();
                Console.WriteLine("PullConsumer: PASS=" + _pass + " FAIL=" + _fail);
                consumer.Shutdown();
                return 1;
            }
        }

        Check("S1 拿到 " + QueueNum.ToString(CultureInfo.InvariantCulture) + " 个队列", true,
            "got=" + routes.Count.ToString(CultureInfo.InvariantCulture));
        {
            bool namesOk = routes.All(q => q.Topic == Topic && q.BrokerName.Length > 0);
            Check("S1 队列 topic 与 broker 名非空", namesOk, "sample=" + QKey(routes[0]));
        }

        // ---------------- S2 生产 ----------------
        Console.WriteLine();
        Console.WriteLine("S2 生产 " + NMsg.ToString(CultureInfo.InvariantCulture) + " 条");
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

            Check("S2 生产 " + NMsg.ToString(CultureInfo.InvariantCulture) + " 条成功", sentOk == NMsg,
                "sentOk=" + sentOk.ToString(CultureInfo.InvariantCulture));
            prod.Shutdown();
        }

        // 轮询等 consumequeue 分发落地（不能刚发完就查 maxOffset）
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
            Check("S2 队列 " + QKey(q) + " 有 " + PerQueue.ToString(CultureInfo.InvariantCulture) + " 条",
                diff == PerQueue,
                "min=" + lo[QKey(q)].ToString(CultureInfo.InvariantCulture)
                    + " max=" + hi[QKey(q)].ToString(CultureInfo.InvariantCulture));
        }

        // ---------------- S3 手动拉取 ----------------
        Console.WriteLine();
        Console.WriteLine("S3 手动拉取（逐队列 min -> max）");
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
                    Check("S3 拉取 " + QKey(q) + " 异常", false, e.Message);
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
                    Check("S3 拉取 " + QKey(q) + " 状态异常", false, "status=" + r.Status);
                    break;
                }
            }
        }

        {
            var missing = sent.Except(got, StringComparer.Ordinal).OrderBy(x => x, StringComparer.Ordinal);
            var extra = got.Except(sent, StringComparer.Ordinal).OrderBy(x => x, StringComparer.Ordinal);
            Check("S3 手动拉取收全 " + NMsg.ToString(CultureInfo.InvariantCulture) + " 条且内容一致",
                got.SetEquals(sent),
                "got=" + got.Count.ToString(CultureInfo.InvariantCulture) + " missing=[" + Join(missing)
                    + "] extra=[" + Join(extra) + "]");
        }

        if (got.Count == 0)
        {
            Console.WriteLine();
            Console.WriteLine("!! 一条都没拉到，后续场景跳过");
            Console.WriteLine();
            Console.WriteLine("PullConsumer: PASS=" + _pass + " FAIL=" + _fail);
            consumer.Shutdown();
            return 1;
        }

        // ---------------- S4 手动提交位点 ----------------
        Console.WriteLine();
        Console.WriteLine("S4 手动提交位点并回读");
        {
            MessageQueue q0 = routes[0];
            long target = hi[QKey(q0)];
            consumer.UpdateConsumeOffset(q0, target);
            bool haveOffset = consumer.FetchConsumeOffset(q0, out long back);
            Check("S4 位点提交后回读一致", haveOffset && back == target,
                "committed=" + back.ToString(CultureInfo.InvariantCulture)
                    + " target=" + target.ToString(CultureInfo.InvariantCulture));
        }

        // ---------------- S5 位点由调用方掌控 ----------------
        Console.WriteLine();
        Console.WriteLine("S5 位点由调用方掌控");
        {
            MessageQueue q0 = routes[0];
            long committed = hi[QKey(q0)];
            PullResult r1 = consumer.Pull(q0, "*", committed, 32, 5000);
            Check("S5 从已提交位点再拉 = NoNewMsg",
                r1.Status == PullStatus.NoNewMsg && r1.MsgFoundList.Count == 0,
                "status=" + r1.Status + " n="
                    + r1.MsgFoundList.Count.ToString(CultureInfo.InvariantCulture));

            PullResult r2 = consumer.Pull(q0, "*", lo[QKey(q0)], 32, 5000);
            Check("S5 位点退回 min 后可重拉（pull 模式的核心能力）",
                r2.Status == PullStatus.Found && r2.MsgFoundList.Count > 0,
                "status=" + r2.Status + " n="
                    + r2.MsgFoundList.Count.ToString(CultureInfo.InvariantCulture));
        }

        // ---------------- S6 位点查询 ----------------
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

        // ---------------- S7 回投 ----------------
        Console.WriteLine();
        Console.WriteLine("S7 SendMessageBack -> %RETRY%group 可拉取");
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
                Check("S7 取样本消息", false, "no message available");
            }
            else
            {
                string want = BodyOf(sample);
                try
                {
                    consumer.SendMessageBack(sample, 0);
                    Check("S7 回投请求被 broker 接受", true,
                        "offset=" + sample.CommitLogOffset.ToString(CultureInfo.InvariantCulture)
                            + " body=" + want);
                }
                catch (Exception e)
                {
                    Check("S7 回投请求被 broker 接受", false, e.Message);
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

                Check("S7 %RETRY% 拉到了被回投的消息", found, detail);
            }
        }

        consumer.Shutdown();

        Console.WriteLine();
        Console.WriteLine("PullConsumer: PASS=" + _pass + " FAIL=" + _fail);
        return _fail == 0 ? 0 : 1;
    }
}
