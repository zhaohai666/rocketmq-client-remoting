// 异步发送内核（Java DefaultMQProducerImpl 的 ASYNC 分支 + MQClientAPIImpl.sendMessageAsync/
// onExceptionImpl）真机验证。
// 用法：rmq async-send [namesrv]
//
// 前置：NameServer + Broker 已起，autoCreateTopicEnable=true。
//
// 离线单测（tests/RocketMQ.Client.Tests/ProducerAsyncTests.cs，28 项）锁的是链的形状：
// 调用方立刻返回、钩子各跑一次、预算共享、闸门/队满怎么拒。那些场景真集群造不出来
//（造不出 SYSTEM_BUSY，也造不出慢 broker），但离线对拍也证不了真 broker 上的三件事：
//   A1  「立刻返回」是真的：before 钩子睡 400ms 时调用方仍在 200ms 内返回；这一笔最后
//       SEND_OK，且用 broker 回的 offsetMsgId 能 **viewMessage 读回原 body 和 queueOffset**
//       （回调里的 SendResult 不是自说自话）；before 钩子在 AsyncSenderExecutor_N 上跑、
//       用户回调在 NettyClientPublicExecutor_N 上跑（Java executeInvokeCallback 的线程口径）。
//   A2  并发 30 笔异步发送：**每笔恰好一个终态**、全部 SEND_OK、broker 上正好落 30 条，
//       并且各笔的 queueOffset 互不重叠（串台的话两个回调会指向同一个位置）。
//   A3  定点发送（给了 mq）真的落在那条队列上，别的队列一条都不多。
//   A4  拦截钩子（CheckForbidden）看到的是 CommunicationMode.Async；它拒绝时异常原样到
//       回调，而且 broker 上一条都没落（连请求都没发出去）。
//   A5  批量异步（SendBatchAsync，对位 Java send(Collection, SendCallback, timeout)）：
//       一批 3 条回调恰好一次 + broker 侧真落 3 条；**逐条客户端 ID 真的存进了 broker**
//       （读回子消息看 UNIQ_KEY）；定点批量真的落在指定队列；混 topic / 空批的本地校验
//       没被异步路径绕过（错误**进回调**，一次都不欠）；背压扣的是**整批**字节，两份许可原样归还。
//   A6  Shutdown 排空在途准备段（Java/Python 用的是不等待的 shutdown()，队列里的任务会
//       连同任务一起被丢掉）：交进来的每一笔都真的上线了，broker 上条条落地。
//
// ⚠ 三条脚本纪律（沿用 LiveBackPressure 那一轮真机联调踩出来的口径）：
//   ① topic 一律带 stamp，避免继承上一轮的条数；
//   ② 「有没有落地」只看 broker 侧各队列 maxOffset-minOffset 之和，不看客户端回调；
//   ③ 新建 topic 要等 broker 把 topicConfig 注册到 namesrv（秒级到十秒级），所以所有
//      broker 侧对账都是**轮询到超时**，读一次路由失败不算失败。
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;

namespace RocketMQ.Examples;

public static class LiveAsyncSend
{
    private static readonly string Stamp =
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);

    private static int _pass;
    private static int _fail;
    private static readonly List<string> Failed = new();

    private static string N(long v) => v.ToString(CultureInfo.InvariantCulture);

    private static void Check(string name, bool ok, string detail = "")
    {
        if (ok)
        {
            _pass++;
            Console.WriteLine("  [PASS] " + name + (detail.Length == 0 ? "" : "  " + detail));
        }
        else
        {
            _fail++;
            Failed.Add(name);
            Console.WriteLine("  [FAIL] " + name + (detail.Length == 0 ? "" : "  " + detail));
        }
    }

    private static void Skip(string name, string why)
    {
        Console.WriteLine("  [SKIP] " + name + "  " + why);
    }

    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 50)
    {
        long deadline = NowMs() + timeoutMs;
        while (NowMs() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(intervalMs);
        }

        return pred();
    }

    private static long NowMs() => (long)UtilAll.MonotonicMillis();

    /// <summary>回调记账：终态条数、SEND_OK 条数、结果、异常文本，以及**跑在哪根线程上**
    /// （Java 的 executeInvokeCallback 口径只能这么验）。</summary>
    private sealed class Recorder : ISendCallback
    {
        private readonly object _lk = new();
        private readonly List<SendResult> _results = new();
        private readonly List<string> _errors = new();

        public string LastThreadName { get; private set; } = string.Empty;

        public int Done
        {
            get { lock (_lk) return _results.Count + _errors.Count; }
        }

        public int Ok
        {
            get { lock (_lk) return _results.Count(r => r.SendStatus == SendStatus.SendOk); }
        }

        public int ErrorCount
        {
            get { lock (_lk) return _errors.Count; }
        }

        public List<SendResult> Results()
        {
            lock (_lk) return new List<SendResult>(_results);
        }

        public string FirstError()
        {
            lock (_lk) return _errors.Count == 0 ? string.Empty : _errors[0];
        }

        public bool WaitDone(int n, int millis) => WaitUntil(() => Done >= n, millis);

        public string Summary()
        {
            lock (_lk)
            {
                return "done=" + N(Done) + " ok=" + N(Ok) + " err=" + N(_errors.Count)
                    + (_errors.Count == 0 ? "" : " " + _errors[0]);
            }
        }

        public void OnSuccess(SendResult sendResult)
        {
            lock (_lk)
            {
                _results.Add(sendResult);
                LastThreadName = Thread.CurrentThread.Name ?? string.Empty;
            }
        }

        public void OnException(string error)
        {
            lock (_lk)
            {
                _errors.Add(error);
                LastThreadName = Thread.CurrentThread.Name ?? string.Empty;
            }
        }
    }

    /// <summary>before 钩子睡一段时间：把「准备段」拖慢，才能证明调用方没等它。
    /// 顺便记下两件事：钩子跑在哪根线程上、before/after 各跑了几次。</summary>
    private sealed class TracingHook : ISendMessageHook
    {
        private readonly int _parkMillis;
        private int _before;
        private int _after;
        private string _beforeThread = string.Empty;

        public TracingHook(int parkMillis = 0) => _parkMillis = parkMillis;

        public int Before() => Volatile.Read(ref _before);

        public int After() => Volatile.Read(ref _after);

        public string BeforeThread() => Volatile.Read(ref _beforeThread);

        public string HookName() => "async-tracing";

        public void SendMessageBefore(SendMessageContext context)
        {
            Interlocked.Increment(ref _before);
            Volatile.Write(ref _beforeThread, Thread.CurrentThread.Name ?? string.Empty);
            if (_parkMillis > 0) Thread.Sleep(_parkMillis);
        }

        public void SendMessageAfter(SendMessageContext context)
        {
            Interlocked.Increment(ref _after);
        }
    }

    /// <summary>只拒绝带 forbidden 标签的消息，并记下它看到的 CommunicationMode。</summary>
    private sealed class ForbiddenTagHook : ICheckForbiddenHook
    {
        private int _calls;
        private volatile CommunicationMode _mode = CommunicationMode.Sync;

        public int Calls() => Volatile.Read(ref _calls);

        public CommunicationMode Mode() => _mode;

        public string HookName() => "async-forbidden";

        public void CheckForbidden(CheckForbiddenContext context)
        {
            Interlocked.Increment(ref _calls);
            _mode = context.CommunicationMode;
            if (context.Message is not null && context.Message.Tags == "forbidden")
            {
                throw new MQClientException("live test: tag forbidden is not allowed");
            }
        }
    }

    private sealed class Env
    {
        public string Namesrv = "127.0.0.1:9876";
        public DefaultMQAdminExt Admin = new();

        public string Topic(string prefix) => prefix + "_" + Stamp;
    }

    private static DefaultMQProducer MakeProducer(Env env, string instance,
        params object[] hooks)
    {
        var p = new DefaultMQProducer("PID_rmq_async_dotnet_" + Stamp)
        {
            NamesrvAddr = env.Namesrv,
            InstanceName = "async-dotnet-" + instance + "-" + Stamp,
        };
        foreach (object h in hooks)
        {
            switch (h)
            {
                case ISendMessageHook m:
                    p.RegisterSendMessageHook(m);
                    break;
                case ICheckForbiddenHook f:
                    p.RegisterCheckForbiddenHook(f);
                    break;
            }
        }

        p.Start();
        return p;
    }

    private static Message Msg(string topic, string body) =>
        new(topic, Encoding.UTF8.GetBytes(body));

    /// <summary>broker 上这个 topic 一共落了多少条（各队列 maxOffset-minOffset 之和）；
    /// 读不到路由返回 -1（新 topic 注册到 namesrv 是秒级的）。</summary>
    private static long Landed(Env env, string topic)
    {
        List<MessageQueue> queues;
        try
        {
            queues = env.Admin.ExamineTopicRoute(topic).GetAllMessageQueue(topic);
        }
        catch (Exception)
        {
            return -1;
        }

        long total = 0;
        foreach (MessageQueue mq in queues)
        {
            try
            {
                total += env.Admin.MaxOffset(mq) - env.Admin.MinOffset(mq);
            }
            catch (Exception)
            {
                // 该队列刚建出来还没写过
            }
        }

        return total;
    }

    private static long WaitLanded(Env env, string topic, long expected, int seconds = 30)
    {
        long deadline = NowMs() + seconds * 1000L;
        long landed = Landed(env, topic);
        while (landed < expected && NowMs() < deadline)
        {
            Thread.Sleep(500);
            landed = Landed(env, topic);
        }

        return landed;
    }

    private static long QueueLanded(Env env, MessageQueue mq)
    {
        try
        {
            return env.Admin.MaxOffset(mq) - env.Admin.MinOffset(mq);
        }
        catch (Exception)
        {
            return -1;
        }
    }

    // ------------------------------------------------- A1 立刻返回 + 线程口径 + 读回
    private static void A1NonBlockingAndReadBack(Env env, string t)
    {
        var hook = new TracingHook(parkMillis: 400);
        DefaultMQProducer p = MakeProducer(env, "a1", hook);
        var rec = new Recorder();
        const string body = "async-a1-readback";
        try
        {
            long began = NowMs();
            p.SendAsync(Msg(t, body), rec, 5000);
            long callerTook = NowMs() - began;
            Check("A1 调用方在准备段（钩子睡 400ms）之前就已返回",
                callerTook < 200 && hook.Before() <= 1,
                "调用方耗时=" + N(callerTook) + "ms before 已跑=" + N(hook.Before()));
            Check("A1 回调恰好一次且 SEND_OK",
                rec.WaitDone(1, 20000) && rec.Ok == 1 && rec.ErrorCount == 0, rec.Summary());
            SendResult r = rec.Results()[0];
            Check("A1 before 钩子在 AsyncSenderExecutor_N 上跑",
                hook.BeforeThread().StartsWith("AsyncSenderExecutor_", StringComparison.Ordinal),
                "线程名=" + hook.BeforeThread());
            Check("A1 用户回调在 NettyClientPublicExecutor_N 上跑（Java executeInvokeCallback）",
                rec.LastThreadName.StartsWith("NettyClientPublicExecutor_", StringComparison.Ordinal),
                "线程名=" + rec.LastThreadName);
            Check("A1 before/after 各跑一次", hook.Before() == 1 && hook.After() == 1,
                "before=" + N(hook.Before()) + " after=" + N(hook.After()));
            Check("A1 broker 落了这一条", WaitLanded(env, t, 1) == 1);
            // offsetMsgId 是 broker 给的，只有它能解出 commitLog 偏移 ⇒ 读回来对 body
            MessageExt? back = null;
            bool read = WaitUntil(() =>
            {
                try
                {
                    back = env.Admin.ViewMessage(t, r.OffsetMsgId);
                    return back is not null;
                }
                catch (Exception)
                {
                    return false;  // commitLog 还没刷出去
                }
            }, 15000);
            Check("A1 用回调里的 offsetMsgId 能读回这条消息",
                read && back is not null
                && Encoding.UTF8.GetString(back!.Body) == body,
                "body=" + (back is null ? "<null>" : Encoding.UTF8.GetString(back.Body)));
            Check("A1 回调里的 queueOffset 就是它落在的位置",
                r.QueueOffset == env.Admin.MaxOffset(r.MessageQueue) - 1,
                "queueOffset=" + N(r.QueueOffset)
                + " maxOffset=" + N(env.Admin.MaxOffset(r.MessageQueue)));
            // MsgId 是客户端补的 UNIQ_KEY（32 位十六进制），不是 broker 的那份
            Check("A1 MsgId 是客户端 UNIQ_KEY、与 broker 的 offsetMsgId 不同",
                r.MsgId.Length == 32 && r.MsgId != r.OffsetMsgId,
                "msgId=" + r.MsgId + " offsetMsgId=" + r.OffsetMsgId);
        }
        finally
        {
            p.Shutdown();
        }
    }

    // ------------------------------------------------- A2 并发不串台
    private static void A2BurstExactlyOnceEach(Env env, string t)
    {
        const int burst = 30;
        DefaultMQProducer p = MakeProducer(env, "a2");
        var each = new Recorder[burst];
        var threads = new List<Thread>();
        try
        {
            for (int i = 0; i < burst; i++)
            {
                int idx = i;
                var rec = new Recorder();
                each[i] = rec;
                var th = new Thread(() => p.SendAsync(Msg(t, "async-burst-" + N(idx)), rec, 8000))
                {
                    IsBackground = true,
                };
                th.Start();
                threads.Add(th);
            }

            foreach (Thread th in threads) th.Join(30000);
            bool allBack = WaitUntil(() => each.All(r => r.Done >= 1), 20000);
            Check("A2 30 笔并发异步发送每笔都拿到终态", allBack,
                "done=" + N(each.Sum(r => r.Done)));
            Check("A2 每笔**恰好一个**终态（不多不少）",
                each.All(r => r.Done == 1),
                "多拿回调的笔数=" + N(each.Count(r => r.Done > 1)));
            Check("A2 全部 SEND_OK",
                each.All(r => r.Ok == 1 && r.ErrorCount == 0),
                each[0].Summary());
            Check("A2 broker 上正好落 30 条", WaitLanded(env, t, burst) == burst);
            List<SendResult> rs = each.SelectMany(r => r.Results()).ToList();
            HashSet<string> slots = rs
                .Select(r => r.MessageQueue.BrokerName + "#" + N(r.MessageQueue.QueueId)
                                      + "@" + N(r.QueueOffset))
                .ToHashSet(StringComparer.Ordinal);
            Check("A2 各笔的 (broker, queueId, queueOffset) 互不重叠",
                slots.Count == burst, "去重后=" + N(slots.Count));
            Check("A2 每笔的 UNIQ_KEY 都不一样",
                rs.Select(r => r.MsgId).Distinct(StringComparer.Ordinal).Count() == burst);
        }
        finally
        {
            p.Shutdown();
        }
    }

    // ------------------------------------------------- A3 定点发送
    private static void A3PinnedQueue(Env env, string t)
    {
        DefaultMQProducer p = MakeProducer(env, "a3");
        var rec = new Recorder();
        try
        {
            // 先把 topic 撑出来（同步发一笔，让 broker 把队列建全），再挑一条定点打。
            // ⚠ 取基线之前必须等预热那一笔在 broker 侧**已经可读**：刚 ack 的报文落到
            // consumeQueue 有延迟，基线读成 0、对账时它变成 1，会凭空多出 1 条。
            p.Send(Msg(t, "async-a3-warmup"), 5000);
            Check("A3 预热那一笔已经在 broker 上可读", WaitLanded(env, t, 1) >= 1,
                "landed=" + N(Landed(env, t)));
            List<MessageQueue> queues = p.FetchPublishMessageQueues(t);
            Check("A3 取到了发布队列", queues.Count > 0,
                "queues=" + N(queues.Count));
            MessageQueue aimed = queues[0];
            long before = QueueLanded(env, aimed);
            long othersBefore = queues.Skip(1).Sum(q => Math.Max(0, QueueLanded(env, q)));
            p.SendAsync(Msg(t, "async-a3-pinned"), rec, 5000, aimed);
            Check("A3 定点异步发送拿到终态且 SEND_OK",
                rec.WaitDone(1, 20000) && rec.Ok == 1, rec.Summary());
            SendResult r = rec.Results()[0];
            Check("A3 结果落在指定的那条队列上",
                r.MessageQueue.BrokerName == aimed.BrokerName
                && r.MessageQueue.QueueId == aimed.QueueId,
                "broker=" + r.MessageQueue.BrokerName + " queueId=" + N(r.MessageQueue.QueueId));
            Check("A3 那条队列正好多 1 条",
                WaitUntil(() => QueueLanded(env, aimed) == before + 1, 20000),
                "landed=" + N(QueueLanded(env, aimed)) + " 之前=" + N(before));
            long othersAfter = queues.Skip(1).Sum(q => Math.Max(0, QueueLanded(env, q)));
            Check("A3 别的队列一条都没多", othersAfter == othersBefore,
                "其它队列 " + N(othersBefore) + " -> " + N(othersAfter));
        }
        finally
        {
            p.Shutdown();
        }
    }

    // ------------------------------------------------- A4 拦截钩子
    private static void A4ForbiddenHook(Env env, string t)
    {
        var forbidden = new ForbiddenTagHook();
        DefaultMQProducer p = MakeProducer(env, "a4", forbidden);
        var rejected = new Recorder();
        var passed = new Recorder();
        try
        {
            Message bad = Msg(t, "async-a4-rejected");
            bad.Tags = "forbidden";
            p.SendAsync(bad, rejected, 5000);
            Check("A4 钩子拒绝的异常原样到了回调",
                rejected.WaitDone(1, 10000)
                && rejected.FirstError().Contains("tag forbidden is not allowed",
                    StringComparison.Ordinal),
                rejected.Summary());
            Check("A4 拦截钩子看到的是 ASYNC", forbidden.Mode() == CommunicationMode.Async,
                "mode=" + forbidden.Mode());
            // 这个 topic 除了被拒的这一笔什么都没有 ⇒ 要么读到 0 条，要么连路由都还没
            // 注册上（-1）。路由是**第一条消息落到 broker** 才会被 autoCreate 建出来的，
            // 所以「读不到路由」本身就是「broker 没收到过请求」的证据。
            long afterReject = Landed(env, t);
            Check("A4 被拒的这笔在 broker 上没留痕", afterReject <= 0,
                "landed=" + N(afterReject) + "（-1 = 路由还没建出来，即 broker 一条都没收到）");

            p.SendAsync(Msg(t, "async-a4-ok"), passed, 5000);
            Check("A4 同一个生产者换个标签照常落地（拒绝没把池子弄坏）",
                passed.WaitDone(1, 20000) && passed.Ok == 1, passed.Summary());
            Check("A4 broker 上正好落 1 条", WaitLanded(env, t, 1) == 1,
                "landed=" + N(Landed(env, t)));
            Check("A4 钩子一共被调 2 次（一笔被拒、一笔放行）", forbidden.Calls() == 2,
                "calls=" + N(forbidden.Calls()));
        }
        finally
        {
            p.Shutdown();
        }
    }

    // ------------------------------------------------- A5 批量走同步批量内核
    private static long OthersLanded(Env env, List<MessageQueue> queues)
    {
        long total = 0;
        for (int i = 1; i < queues.Count; i++)
        {
            total += Math.Max(0, QueueLanded(env, queues[i]));
        }

        return total;
    }

    /// <summary>
    /// 批量异步入口（对位 Java <c>send(Collection&lt;Message&gt;, SendCallback, long)</c>、Python
    /// <c>send_async(list)</c>）在真 broker 上的五条口径。每条都刻意选在「只跑离线单测看不出来」
    /// 的那一侧：回调有没有交付、逐条客户端 ID 有没有真的存进 broker、本地校验有没有被异步路径
    /// 绕过、错误有没有被静默吞掉、字节许可有没有漏。
    /// </summary>
    private static void A5BatchAsync(Env env, string t)
    {
        DefaultMQProducer p = MakeProducer(env, "a5");

        // ① 一批 3 条：回调恰好一次、SEND_OK，而且 broker 上真的落了 3 条。
        //    只看回调会被「回调报了 OK 但请求压根没发出去」蒙过去 —— 那条必须靠 broker 侧对账。
        var rec = new Recorder();
        var list = new List<Message>
        {
            Msg(t, "async-a5-0"), Msg(t, "async-a5-1"), Msg(t, "async-a5-2"),
        };
        p.SendBatchAsync(list, rec, 10000);
        Check("A5 一批 3 条：回调恰好一次且 SEND_OK",
            rec.WaitDone(1, 20000) && rec.Done == 1 && rec.Ok == 1
                && rec.Results().Count == 1 && rec.Results()[0].MsgId.Length == 32, rec.Summary());
        long landed = WaitLanded(env, t, 3);
        Check("A5 broker 上真落了 3 条（不是回调自己说成功）", landed == 3,
            "landed=" + N(landed) + "（-1 = 路由还没建出来）");
        Skip("A5 请求码 = SEND_BATCH_MESSAGE(320)",
            "真机看不到上线报文，这一条由 ProducerAsyncTests.BatchAsync 在进程内取证");

        // ①b 逐条 ID 的**落地**证据：读回 broker 存下来的那条子消息，它必须带客户端生成的
        //     32 位 UNIQ_KEY。Java batch():1176 的顺序是「逐条 setUniqID → 才 encode()」；
        //     顺序错了（或像修复前的本端口那样压根不写），broker 拆开批量后存的就是没有 ID 的
        //     裸消息 —— 消费端去重、轨迹控制台串线全废，而发送侧回调照样 SEND_OK，看不出来。
        SendResult batchResult = rec.Results().Count > 0 ? rec.Results()[0] : new SendResult();
        Check("A5 批量的 MsgId 是客户端 32 位 ID、不是 broker 的 OffsetMsgId",
            batchResult.MsgId.Length == 32 && !batchResult.MsgId.Contains(',')
                && batchResult.MsgId != batchResult.OffsetMsgId,
            "MsgId=" + batchResult.MsgId + " OffsetMsgId=" + batchResult.OffsetMsgId);
        // 批量应答的 OffsetMsgId 是 broker **逐条**回的一串（逗号分隔，一条子消息一个
        // commitLog 偏移），它本身就是「这一批被拆开存成 3 条」的证据
        string[] subOffsets = Array.FindAll(
            batchResult.OffsetMsgId.Split(','), s => s.Length > 0);
        Check("A5 broker 逐条回了 3 个 commitLog 偏移（批量确实被拆开落地）",
            subOffsets.Length == 3, "OffsetMsgId=" + batchResult.OffsetMsgId);
        string storedUniq = string.Empty;
        if (subOffsets.Length > 0)
        {
            MessageExt? storedSub = null;
            Check("A5 能读回 broker 上存下来的那条子消息",
                WaitUntil(() =>
                {
                    try
                    {
                        storedSub = env.Admin.ViewMessage(t, subOffsets[0]);
                        return true;
                    }
                    catch (Exception)
                    {
                        return false; // commitLog 还没刷出去
                    }
                }, 15000), "offset=" + subOffsets[0]);
            // MessageConst.PropertyUniqClientMessageIdKeyidx == "UNIQ_KEY"
            storedUniq = storedSub?.GetProperty("UNIQ_KEY") ?? string.Empty;
        }

        Check("A5 broker 上存的子消息带客户端 UNIQ_KEY（逐条 ID 编在 body 里）",
            storedUniq.Length == 32, "stored UNIQ_KEY=" + storedUniq);

        // ② 定点批量：mq 参数真的传到了批量内核，而不是被丢掉后自己挑一条。
        List<MessageQueue> queues;
        try
        {
            queues = env.Admin.ExamineTopicRoute(t).GetAllMessageQueue(t);
        }
        catch (Exception e)
        {
            Check("A5 定点批量取到了发布队列", false, e.Message);
            p.Shutdown();
            return;
        }

        if (queues.Count == 0)
        {
            Check("A5 定点批量取到了发布队列", false, "queues=0");
            p.Shutdown();
            return;
        }

        MessageQueue aimed = queues[0];
        long aimedBefore = QueueLanded(env, aimed);
        long othersBefore = OthersLanded(env, queues);
        var pinned = new Recorder();
        p.SendBatchAsync(
            new List<Message> { Msg(t, "async-a5-pin-0"), Msg(t, "async-a5-pin-1"), Msg(t, "async-a5-pin-2") },
            pinned, 10000, aimed);
        bool pinnedOk = pinned.WaitDone(1, 20000) && pinned.Done == 1 && pinned.Ok == 1;
        List<SendResult> pr = pinned.Results();
        bool landedOnAimed = pr.Count > 0 && pr[0].MessageQueue.BrokerName == aimed.BrokerName
            && pr[0].MessageQueue.QueueId == aimed.QueueId;
        long aimedAfter = QueueLanded(env, aimed);
        Check("A5 定点批量那条队列正好多 3 条",
            WaitUntil(() => (aimedAfter = QueueLanded(env, aimed)) == aimedBefore + 3, 20000),
            "该队列 " + N(aimedBefore) + " -> " + N(aimedAfter));
        long othersAfter = OthersLanded(env, queues);
        Check("A5 定点批量落在指定队列、别的队列一条都没多",
            pinnedOk && landedOnAimed && aimedAfter == aimedBefore + 3 && othersAfter == othersBefore,
            "ok=" + N(pinned.Ok) + " 落位=" + (landedOnAimed ? "1" : "0")
                + " 该队列 " + N(aimedBefore) + "->" + N(aimedAfter)
                + " 其它 " + N(othersBefore) + "->" + N(othersAfter));

        // ③ 混 topic 的一批：批量内核的同质性校验必须在异步路径上照样跑，且异常**进回调**
        //    （Java 的 runnable catch → onException），不是同步抛、也不是静默丢掉。
        var mixed = new Recorder();
        p.SendBatchAsync(
            new List<Message> { Msg(t, "async-a5-mixed-0"), Msg(env.Topic("BatchOther"), "async-a5-mixed-1") },
            mixed, 5000);
        Check("A5 混 topic 的一批在回调里报错（本地校验没被异步路径绕过）",
            mixed.WaitDone(1, 10000) && mixed.Done == 1 && mixed.Ok == 0
                && mixed.FirstError().Contains("should be the same"), mixed.Summary());

        // ④ 空批次：同一口径 —— 错误进回调，一次回调都不欠。
        var empty = new Recorder();
        p.SendBatchAsync(new List<Message>(), empty, 5000);
        Check("A5 空批次也是「恰好一次失败回调」，不静默吞掉",
            empty.WaitDone(1, 10000) && empty.Done == 1 && empty.Ok == 0
                && empty.FirstError().Contains("message list is empty"), empty.Summary());

        // ⑤ 背压扣的是**整批**字节，不是「一条」：字节闸压到地板（1 MiB），一批 2×600 KiB
        //    必须被拦下（整批 1.2 MiB > 1 MiB）；换一批 2×100 KiB 又必须过。只按第一条算
        //    （600 KiB）或者干脆不扣，这两条就会同时反向。
        p.EnableBackpressureForAsyncMode = true;
        p.BackPressureForAsyncSendNum = 1;
        p.BackPressureForAsyncSendSize = 1024 * 1024;
        var gated = new Recorder();
        p.SendBatchAsync(
            new List<Message> { Msg(t, new string('x', 600 * 1024)), Msg(t, new string('x', 600 * 1024)) },
            gated, 2000);
        Check("A5 整批 1.2MiB 被 1MiB 字节闸拦下（回调拿到 TOO_MUCH_REQUEST）",
            gated.WaitDone(1, 15000) && gated.Done == 1 && gated.Ok == 0
                && gated.FirstError().Contains("semaphoreAsyncSize timeout"), gated.Summary());
        // 同一道闸下的小批次必须照常落地：证明「拦住」是因为整批量，不是因为批量被一概论处
        var passed = new Recorder();
        p.SendBatchAsync(
            new List<Message> { Msg(t, new string('y', 100 * 1024)), Msg(t, new string('y', 100 * 1024)) },
            passed, 10000);
        Check("A5 同一道闸下 200KiB 的一批照常拿到 SEND_OK",
            passed.WaitDone(1, 20000) && passed.Done == 1 && passed.Ok == 1, passed.Summary());
        // 许可必须原样归还：漏一份就是长跑之后「所有异步发送集体超时」，而单次发送看不出来。
        int numTotal = p.GetBackPressureForAsyncSendNum();
        int sizeTotal = p.GetBackPressureForAsyncSendSize();
        Check("A5 批量路径把两份许可原样归还（空闲量回到总量，含被闸拦下那一笔）",
            WaitUntil(() => p.SemaphoreAsyncSendNumAvailablePermits == numTotal
                && p.SemaphoreAsyncSendSizeAvailablePermits == sizeTotal, 10000),
            "空闲条数=" + N(p.SemaphoreAsyncSendNumAvailablePermits) + " 总量=" + N(numTotal)
                + " 空闲字节=" + N(p.SemaphoreAsyncSendSizeAvailablePermits) + " 总量=" + N(sizeTotal));
        p.Shutdown();
    }

    // ------------------------------------------------- A6 关池排空在途准备段
    private static void A6ShutdownDrains(Env env, string t)
    {
        int sends = Math.Max(1, Environment.ProcessorCount) * 3;
        var hook = new TracingHook(parkMillis: 100);
        DefaultMQProducer p = MakeProducer(env, "a6", hook);
        var rec = new Recorder();
        try
        {
            for (int i = 0; i < sends; i++)
            {
                p.SendAsync(Msg(t, "async-a6-" + N(i)), rec, 8000);
            }

            // 立刻关：Java/Python 在这里会把队列里没跑到的任务连人带回调一起丢掉
            p.Shutdown();
            long landed = WaitLanded(env, t, sends, 40);
            Check("A6 Shutdown 排空了队列：交进来的每一笔都上线了",
                landed == sends, "landed=" + N(landed) + " 发送=" + N(sends));
            Check("A6 一笔至多一个终态回调", rec.Done <= sends,
                "done=" + N(rec.Done) + " 发送=" + N(sends));
        }
        catch (Exception e)
        {
            // Shutdown 已经被调过，finally 里不能再关一次
            Check("A6 Shutdown 排空在途准备段", false, e.Message);
        }

        p = MakeProducer(env, "a6-after");
        var after = new Recorder();
        p.SendAsync(Msg(t, "async-a6-after"), after, 8000);
        Check("A6 关掉的池子不会被别的生产者复用（新生产者接着能发）",
            after.WaitDone(1, 20000) && after.Ok == 1, after.Summary());
        p.Shutdown();
    }

    private static List<string> BrokerAddrs(Env env)
    {
        try
        {
            return env.Admin.FetchBrokerClusterInfo().GetBrokerAddrs();
        }
        catch (Exception)
        {
            return new List<string>();
        }
    }

    public static int Run(string[] args)
    {
        var env = new Env
        {
            Namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876",
            Admin = new DefaultMQAdminExt("ADMIN_async"),
        };
        env.Admin.SetNamesrvAddr(env.Namesrv);
        env.Admin.SetTimeoutMillis(10000);
        string[] topics =
        {
            env.Topic("AsyncReadBack"), env.Topic("AsyncBurst"), env.Topic("AsyncPinned"),
            env.Topic("AsyncForbidden"), env.Topic("AsyncBatch"), env.Topic("AsyncDrain"),
        };

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("RocketMQ async send kernel live verify (.NET): namesrv="
            + env.Namesrv + " stamp=" + Stamp);
        Console.WriteLine(new string('=', 70));
        try
        {
            env.Admin.Start();
            A1NonBlockingAndReadBack(env, topics[0]);
            A2BurstExactlyOnceEach(env, topics[1]);
            A3PinnedQueue(env, topics[2]);
            A4ForbiddenHook(env, topics[3]);
            A5BatchAsync(env, topics[4]);
            A6ShutdownDrains(env, topics[5]);
        }
        catch (Exception e)
        {
            Check("脚本整体执行", false, e.Message);
        }

        foreach (string addr in BrokerAddrs(env))
        {
            foreach (string topic in topics)
            {
                try
                {
                    env.Admin.DeleteTopicInBroker(addr, topic);
                }
                catch (Exception)
                {
                    // 清理失败不影响结论
                }
            }
        }

        env.Admin.Shutdown();

        Console.WriteLine(new string('#', 60));
        Console.WriteLine("PASS=" + N(_pass) + " FAIL=" + N(_fail));
        foreach (string name in Failed) Console.WriteLine("  FAILED: " + name);
        return _fail == 0 ? 0 : 1;
    }
}
