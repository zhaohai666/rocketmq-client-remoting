// 异步发送背压（EnableBackpressureForAsyncMode 那一套公平信号量）真机验证
// （对齐 Java DefaultMQProducerImpl.executeAsyncMessageSend:635-682，
// 与 python/verify_backpressure_live.py、cpp/examples/live_backpressure.cpp 同套场景）。
// 用法：rmq backpressure [namesrv]
//
// 前置：NameServer + Broker 已起，autoCreateTopicEnable=true。
//
// 离线单测（tests/RocketMQ.Client.Tests/BackPressureTests.cs）锁的是信号量语义；
// 这个脚本锁的是**打到真 broker 时**的五件事：
//   B1  默认容量（1024 条 / 100M 字节）开着背压发一整轮异步消息：全部 SEND_OK，
//       且发完之后两个信号量**满额归还**（真机不泄配额 —— 泄了迟早把生产者自己锁死）。
//   B2  条数闸夹到地板值 10、用 SendMessageBefore 钩子睡 600ms 把在途占满：
//       第 11、12 笔在**调用方线程**上等不到许可，回调
//       send message tryAcquire semaphoreAsyncNum timeout（Java :654-658 原文案），
//       而且 broker 上**一条都没多** —— 被拒的请求连路由都没查。
//   B3  运行时把容量从 10 调到 12：正卡在闸上的调用方被叫醒，broker 上多出那 1 条，
//       全部落地后空闲许可 = 新容量（这一轮钩子睡 2s，留出足够的观察窗口）。
//   B4  字节闸（容量 1M 地板值 + 600KB body ⇒ 在途只能 1 笔）：第二笔回调
//       send message tryAcquire semaphoreAsyncSize timeout（Java :667-671），
//       在途时空闲字节许可正好是 1M - 600K，broker 上只落 1 条。
//   B5  关掉背压：同样的容量配置**完全不限流**，30 笔并发（含 300KB 大 body）全部落地。
//
// ⚠ 三条脚本纪律（Python 那一轮真机联调踩出来的）：
//   ① topic 一律带 Stamp —— 三语言**依次**跑在同一个 broker 上，固定名会继承上一轮的条数；
//   ② 「被拒的发送连请求都没发出去」只能看 broker 侧落库条数（各队列 maxOffset-minOffset 之和），
//      光看客户端回调会被「回调报错但请求照样发出去」的实现蒙过去；
//   ③ 新建 topic 要等 broker 把 topicConfig 增量注册到 namesrv（秒级到十秒级），所以所有
//      broker 侧对账都是**轮询到超时**，读一次路由失败不算失败。
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveBackPressure
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

    /// <summary>线程安全的回调记录（对应 Python 验证脚本里的 _Recorder）。</summary>
    private sealed class Recorder : ISendCallback
    {
        private readonly object _lk = new();
        private readonly List<string> _errors = new();
        private string _firstError = string.Empty;

        public int Done { get; private set; }

        public int Ok { get; private set; }

        public void OnSuccess(SendResult sendResult)
        {
            lock (_lk)
            {
                Done++;
                if (sendResult.SendStatus == SendStatus.SendOk) Ok++;
            }
        }

        public void OnException(string error)
        {
            lock (_lk)
            {
                Done++;
                _errors.Add(error);
                if (_firstError.Length == 0) _firstError = error;
            }
        }

        public int ErrorCount
        {
            get { lock (_lk) return _errors.Count; }
        }

        /// <summary>「每一条错误都带这句文案」—— Java 的文案是逐字对齐的，只查条数会放过内容漂移。</summary>
        public bool AllErrorsContain(string needle, int expected)
        {
            lock (_lk)
            {
                if (_errors.Count != expected || Done != expected) return false;
                return _errors.All(e => e.Contains(needle, StringComparison.Ordinal));
            }
        }

        public string Summary()
        {
            lock (_lk)
            {
                return "ok=" + N(Ok) + " err=" + N(_errors.Count)
                    + (_firstError.Length == 0 ? "" : " " + _firstError);
            }
        }
    }

    /// <summary>
    /// 在 SendMessageBefore 里睡一会儿。许可是**过闸时**拿的、**链终点**还的，所以占住在途
    /// 最干净的办法是让链本身变慢：堵用户回调占不住许可（归还就在把结果交给用户之前一步）。
    /// </summary>
    private sealed class SlowHook : ISendMessageHook
    {
        private long _millis;

        public SlowHook(long millis) => _millis = millis;

        public string HookName() => "slow-before";

        public void SendMessageBefore(SendMessageContext context)
        {
            long ms = Interlocked.Read(ref _millis);
            if (ms > 0) Thread.Sleep((int)ms);
        }

        public void SendMessageAfter(SendMessageContext context)
        {
        }

        public void SetMillis(long ms) => Interlocked.Exchange(ref _millis, ms);
    }

    private sealed class Env
    {
        public string Namesrv = "127.0.0.1:9876";
        public DefaultMQAdminExt Admin = new();

        public string Topic(string prefix) => prefix + "_" + Stamp;
    }

    // 钩子必须在 Start() 之前挂上（Python 验证脚本同样如此）
    private static DefaultMQProducer MakeProducer(Env env, string instance, bool enable,
        long num = -1, long size = -1, ISendMessageHook? hook = null)
    {
        var p = new DefaultMQProducer("PID_rmq_bp_dotnet_" + Stamp)
        {
            NamesrvAddr = env.Namesrv,
            InstanceName = "bp-dotnet-" + instance + "-" + Stamp,
            EnableBackpressureForAsyncMode = enable,
        };
        if (num >= 0) p.BackPressureForAsyncSendNum = (int)num;
        if (size >= 0) p.BackPressureForAsyncSendSize = (int)size;
        if (hook is not null) p.RegisterSendMessageHook(hook);
        p.Start();
        return p;
    }

    private static long NumPermits(DefaultMQProducer p) => p.SemaphoreAsyncSendNumAvailablePermits;

    private static long SizePermits(DefaultMQProducer p) => p.SemaphoreAsyncSendSizeAvailablePermits;

    private static List<Thread> SpawnSenders(DefaultMQProducer p, string topic,
        Func<int, byte[]> bodyOf, int count, ISendCallback cb, int timeout)
    {
        var threads = new List<Thread>();
        for (int i = 0; i < count; i++)
        {
            int idx = i;
            var th = new Thread(() =>
                p.SendAsync(new Message(topic, bodyOf(idx)), cb, timeout))
            {
                IsBackground = true,
            };
            th.Start();
            threads.Add(th);
        }

        return threads;
    }

    private static void JoinAll(List<Thread> threads, double timeoutSeconds = 30.0)
    {
        long deadline = NowMs() + (long)(timeoutSeconds * 1000);
        // Join(0) 在 .NET 里是「无限等待」而不是「立刻返回」，所以剩余时间至少给 1ms
        foreach (Thread th in threads) th.Join((int)Math.Max(1L, deadline - NowMs()));
        int stuck = threads.Count(t => t.IsAlive);
        if (stuck > 0)
        {
            Check("*  发送线程没有卡死", false, stuck + " 条还活着");
        }
    }

    /// <summary>
    /// broker 上这个 topic 一共落了多少条（各队列 maxOffset-minOffset 之和）；读不到路由返回 -1。
    /// 这是「被拒的发送连请求都没发出去」的唯一硬证据。
    /// </summary>
    private static long LandedCount(DefaultMQAdminExt admin, string topic)
    {
        List<MessageQueue> queues;
        try
        {
            queues = admin.ExamineTopicRoute(topic).GetAllMessageQueue(topic);
        }
        catch (Exception)
        {
            return -1;  // 路由还没注册上
        }

        long total = 0;
        foreach (MessageQueue mq in queues)
        {
            try
            {
                total += admin.MaxOffset(mq) - admin.MinOffset(mq);
            }
            catch (Exception)
            {
                // 该队列刚建出来还没写过
            }
        }

        return total;
    }

    /// <summary>等 broker 上至少出现 expected 条，返回最后一次读数（新 topic 注册到 namesrv 是秒级的）。</summary>
    private static long WaitLanded(Env env, string topic, long expected, int seconds = 30)
    {
        long deadline = NowMs() + seconds * 1000L;
        long landed = LandedCount(env.Admin, topic);
        while (landed < expected && NowMs() < deadline)
        {
            Thread.Sleep(500);
            landed = LandedCount(env.Admin, topic);
        }

        if (landed < 0) LandedCount(env.Admin, topic);  // 真读不到时再打一次，便于定位
        return landed;
    }

    private static string BrokerAddr(Env env)
    {
        try
        {
            List<string> addrs = env.Admin.FetchBrokerClusterInfo().GetBrokerAddrs();
            if (addrs.Count > 0) return addrs[0];
        }
        catch (Exception)
        {
            // 拿不到集群信息时用本机默认值
        }

        return "127.0.0.1:10911";
    }

    // ---------------------------------------------------------------- B1 默认容量不漏配额
    private static void B1DefaultCapacityNoLeak(Env env, string t)
    {
        DefaultMQProducer p = MakeProducer(env, "b1", true);
        var rec = new Recorder();
        try
        {
            List<Thread> threads = SpawnSenders(p, t,
                i => Encoding.UTF8.GetBytes("b1-" + N(i) + "-" + new string('x', 120)), 40, rec, 5000);
            JoinAll(threads);
            Check("B1 40 笔异步都走完回调", WaitUntil(() => rec.Done >= 40, 15000), rec.Summary());
            Check("B1 全部 SEND_OK", rec.Ok == 40 && rec.ErrorCount == 0, rec.Summary());
            long landed = WaitLanded(env, t, 40);
            Check("B1 broker 上正好落了 40 条", landed == 40, "landed=" + N(landed));
            Check("B1 条数许可满额归还", NumPermits(p) == 1024, "available=" + N(NumPermits(p)));
            Check("B1 字节许可满额归还", SizePermits(p) == 100L * 1024 * 1024,
                "available=" + N(SizePermits(p)));
        }
        finally
        {
            p.Shutdown();
        }
    }

    // ------------------------------------------------- B2/B3 条数闸：拒绝、对账、扩容
    private static void B2AndB3NumGate(Env env, string t)
    {
        var slow = new SlowHook(600);
        DefaultMQProducer p = MakeProducer(env, "b2", true, FairSemaphore.MinAsyncSendNum, -1, slow);
        var held = new Recorder();
        var rejected = new Recorder();
        try
        {
            List<Thread> holders = SpawnSenders(p, t, _ => Encoding.UTF8.GetBytes("held"),
                (int)FairSemaphore.MinAsyncSendNum, held, 8000);
            Check("B2 在途占满后空闲条数为 0", WaitUntil(() => NumPermits(p) == 0, 3000),
                "available=" + N(NumPermits(p)));

            // 第 11、12 笔：预算只有 150ms，等不到许可
            long began = NowMs();
            List<Thread> rejects = SpawnSenders(p, t, _ => Encoding.UTF8.GetBytes("rejected"),
                2, rejected, 150);
            JoinAll(rejects, 10.0);
            long waited = NowMs() - began;
            Check("B2 闸等到预算耗尽才报错（不是看一眼就拒）", waited >= 130,
                "调用方等了 " + N(waited) + "ms");
            Check("B2 超限的两笔回调 TooMuchRequest，文案与 Java 逐字一致",
                rejected.AllErrorsContain("send message tryAcquire semaphoreAsyncNum timeout", 2),
                rejected.Summary());

            JoinAll(holders, 25.0);
            Check("B2 在途的 10 笔都发出去了",
                WaitUntil(() => held.Ok == (int)FairSemaphore.MinAsyncSendNum, 20000),
                held.Summary());
            long landed2 = WaitLanded(env, t, FairSemaphore.MinAsyncSendNum);
            Check("B2 被拒的两笔在 broker 上一条没留（连请求都没发）",
                landed2 == FairSemaphore.MinAsyncSendNum,
                "landed=" + N(landed2) + " 期望 " + N(FairSemaphore.MinAsyncSendNum));
            Check("B2 全部落地后条数许可回到 10",
                WaitUntil(() => NumPermits(p) == FairSemaphore.MinAsyncSendNum, 20000),
                "available=" + N(NumPermits(p)));

            // B3：再占满 10 个在途，把容量抬到 12 —— 卡在闸上的人应当被叫醒。
            // 这一轮把钩子睡到 2s：占住在途的时间必须远大于「起线程 + 轮询确认 + 起等待方」
            // 这几步的开销，否则检查还没做完整轮就已经归还了。
            slow.SetMillis(2000);
            var round2 = new Recorder();
            List<Thread> holders2 = SpawnSenders(p, t, _ => Encoding.UTF8.GetBytes("held2"),
                (int)FairSemaphore.MinAsyncSendNum, round2, 8000);
            Check("B3 第二轮在途同样占满", WaitUntil(() => NumPermits(p) == 0, 3000),
                "available=" + N(NumPermits(p)));

            var woken = new Recorder();
            // 跨线程读写的可见性：bool 没有 Volatile.Read 重载，用 int + Interlocked
            int waiterReturned = 0;
            var waiter = new Thread(() =>
            {
                p.SendAsync(new Message(t, Encoding.UTF8.GetBytes("woken")), woken, 8000);
                Interlocked.Exchange(ref waiterReturned, 1);
            })
            {
                IsBackground = true,
            };
            waiter.Start();
            Thread.Sleep(300);
            bool passedGate = Interlocked.CompareExchange(ref waiterReturned, 0, 0) != 0;
            Check("B3 扩容前调用方确实卡在闸上", !passedGate && NumPermits(p) == 0,
                "调用方已过闸=" + (passedGate ? "yes" : "no") + " available=" + N(NumPermits(p)));
            p.BackPressureForAsyncSendNum = (int)(FairSemaphore.MinAsyncSendNum + 2);
            waiter.Join(15000);
            // 调用方线程被叫醒就算「过闸了」，但结果要等整条链跑完（这一轮钩子睡 2s）才回到回调，
            // 所以这里等的是回调，不是线程退出。
            Check("B3 扩容把卡在闸上的发送方叫醒并发了出去",
                !waiter.IsAlive && WaitUntil(() => woken.Ok >= 1, 20000), woken.Summary());
            JoinAll(holders2, 25.0);
            Check("B3 全部归还后空闲许可 = 新容量 12",
                WaitUntil(() => NumPermits(p) == FairSemaphore.MinAsyncSendNum + 2, 25000),
                "available=" + N(NumPermits(p)));
            long expect3 = 2 * FairSemaphore.MinAsyncSendNum + 1;
            long landed3 = WaitLanded(env, t, expect3);
            Check("B3 broker 总数 = 两轮在途 + 被叫醒的那一笔", landed3 == expect3,
                "landed=" + N(landed3) + " 期望 " + N(expect3));
        }
        finally
        {
            p.Shutdown();
        }
    }

    // ------------------------------------------------- B4 字节闸（1M 地板 + 600KB）
    private static void B4SizeGate(Env env, string t)
    {
        DefaultMQProducer p = MakeProducer(env, "b4", true, FairSemaphore.MinAsyncSendNum,
            FairSemaphore.MinAsyncSendSize, new SlowHook(400));
        var one = new Recorder();
        var two = new Recorder();
        try
        {
            byte[] big = Encoding.UTF8.GetBytes(new string('4', 600 * 1024));
            long wantInFlight = FairSemaphore.MinAsyncSendSize - big.Length;
            var first = new Thread(() => p.SendAsync(new Message(t, big), one, 8000))
            {
                IsBackground = true,
            };
            first.Start();
            Check("B4 在途字节许可正好扣掉 body 长度",
                WaitUntil(() => SizePermits(p) == wantInFlight, 3000),
                "available=" + N(SizePermits(p)) + " 期望 " + N(wantInFlight));
            var second = new Thread(() => p.SendAsync(new Message(t, big), two, 150))
            {
                IsBackground = true,
            };
            second.Start();
            second.Join(10000);
            Check("B4 第二笔 600KB 过不了字节闸，文案与 Java 逐字一致",
                two.AllErrorsContain("send message tryAcquire semaphoreAsyncSize timeout", 1),
                two.Summary());
            // 字节闸没过时，先前拿到的条数许可必须原样还回去（Java BackpressureSendCallBack:599-610
            // 的先 size 后 num 归还）
            Check("B4 字节闸没过时条数许可已经归还",
                WaitUntil(() => NumPermits(p) == FairSemaphore.MinAsyncSendNum, 3000),
                "available=" + N(NumPermits(p)));
            first.Join(25000);
            Check("B4 第一笔正常落地", WaitUntil(() => one.Done >= 1, 20000), one.Summary());
            long landed = WaitLanded(env, t, 1);
            Check("B4 broker 上只有第一笔（被拒的没留痕）", landed == 1, "landed=" + N(landed));
            Check("B4 全部归还：条数与字节都回到配置额",
                NumPermits(p) == FairSemaphore.MinAsyncSendNum
                && SizePermits(p) == FairSemaphore.MinAsyncSendSize,
                "num=" + N(NumPermits(p)) + " size=" + N(SizePermits(p)));
        }
        finally
        {
            p.Shutdown();
        }
    }

    // ------------------------------------------------- B5 关掉背压就不限流
    private static void B5GateOff(Env env, string t)
    {
        DefaultMQProducer p = MakeProducer(env, "b5", false, FairSemaphore.MinAsyncSendNum,
            FairSemaphore.MinAsyncSendSize, new SlowHook(200));
        var rec = new Recorder();
        try
        {
            // 每三笔里有一笔 300KB：关着背压时连字节闸都不看，开着的话这里必然限流
            List<Thread> threads = SpawnSenders(p, t,
                i => i % 3 == 0 ? Encoding.UTF8.GetBytes(new string('x', 300 * 1024))
                    : Encoding.UTF8.GetBytes("small"),
                30, rec, 8000);
            JoinAll(threads);
            Check("B5 关背压后 30 笔并发全部成功", WaitUntil(() => rec.Done >= 30, 25000),
                rec.Summary());
            Check("B5 全部 SEND_OK", rec.Ok == 30 && rec.ErrorCount == 0, rec.Summary());
            long landed = WaitLanded(env, t, 30);
            Check("B5 broker 上 30 条都在", landed == 30, "landed=" + N(landed));
        }
        finally
        {
            p.Shutdown();
        }
    }

    public static int Run(string[] args)
    {
        var env = new Env
        {
            Namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876",
            Admin = new DefaultMQAdminExt("ADMIN_bp"),
        };
        env.Admin.SetNamesrvAddr(env.Namesrv);
        env.Admin.SetTimeoutMillis(10000);
        string[] topics =
        {
            env.Topic("BpDefault"), env.Topic("BpNumGate"), env.Topic("BpSizeGate"),
            env.Topic("BpOff"),
        };

        Console.WriteLine(new string('=', 70));
        Console.WriteLine("RocketMQ async back-pressure live verify (.NET): namesrv="
            + env.Namesrv + " stamp=" + Stamp);
        Console.WriteLine(new string('=', 70));
        try
        {
            env.Admin.Start();
            B1DefaultCapacityNoLeak(env, topics[0]);
            B2AndB3NumGate(env, topics[1]);
            B4SizeGate(env, topics[2]);
            B5GateOff(env, topics[3]);
        }
        catch (Exception e)
        {
            Check("脚本整体执行", false, e.Message);
        }

        string addr = BrokerAddr(env);
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

        env.Admin.Shutdown();

        Console.WriteLine(new string('#', 60));
        Console.WriteLine("PASS=" + N(_pass) + " FAIL=" + N(_fail));
        foreach (string name in Failed) Console.WriteLine("  FAILED: " + name);
        return _fail == 0 ? 0 : 1;
    }
}
