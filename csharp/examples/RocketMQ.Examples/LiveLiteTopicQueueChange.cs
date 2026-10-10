// LitePull 消费者的 topic 队列集合变更监听真机验证。
// 用法：rmq lite-queue-change [namesrv]
//
// 为什么必须真机：比对趟次每趟都现问 nameserver 取订阅队列集合，而普通路由缓存是 30s
// 才刷一次。假 nameserver 能证比对逻辑，只有真集群能证「现查」。这里把检查周期压到 1s
// 下限、路由轮询保持默认 30s，再把 topic 真的扩容：过了首查延迟的稳定期里，从 nameserver
// 报出新队列数到监听器收到回调只该隔一两趟检查（≤5s）；读 30s 缓存的话这个窗口会拖到半
// 分钟以上。
//
//   L1  队列没动 ⇒ 监听器不被打扰
//   L1b 首查那趟真的跑过 ⇒ 依旧静默（运行中注册的快照不算变化）
//   L2  扩容 2→4：nameserver 认了之后回调紧跟几趟检查
//   L3  回调后快照推进 ⇒ 同一套队列不重复回调
//   L4  缩容 4→2：同样靠现查看到
//   L5  没建过的 topic：取不到队列算「查不到」，不伪装成缩到 0 队列
using System.Globalization;

using RocketMQ.Client;
using RocketMQ.Common;

namespace RocketMQ.Examples;

public static class LiveLiteTopicQueueChange
{
    private const int BaseQueues = 2;
    private const int ScaledQueues = 4;
    // 检查周期压到下限，好把「每趟现查」和「吃 30s 缓存」在时间上分开。
    private const int CheckIntervalMillis = 1000;
    // 后台比对的首查延迟 10s + 余量：此后只剩 1s 一趟的稳定期。
    private const int FirstDelayBudgetMillis = 12000;
    // nameserver 见到变化后允许的最大回调间隔。
    private const int FreshWindowMillis = 5000;

    private static readonly string Stamp =
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString(CultureInfo.InvariantCulture);
    private static readonly string Topic = "LiteQcLiveNet_" + Stamp;
    private static readonly string Group = "LiteQcGNet_" + Stamp;
    private static readonly string Ghost = "LiteQcGhostNet_" + Stamp;

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

    /// <summary>只记回调，不改状态；后台线程也会调它，所以自带锁。</summary>
    private sealed class Recorder : ITopicMessageQueueChangeListener
    {
        private readonly object _gate = new();
        private readonly List<(string Topic, List<int> QueueIds)> _events = new();

        public void OnChanged(string topic, IReadOnlyList<MessageQueue> messageQueues)
        {
            List<int> ids = messageQueues.Select(mq => mq.QueueId).OrderBy(id => id).ToList();
            lock (_gate)
            {
                _events.Add((topic, ids));
            }
        }

        public List<(string Topic, List<int> QueueIds)> Events()
        {
            lock (_gate)
            {
                return new List<(string, List<int>)>(_events);
            }
        }

        public int Count => Events().Count;

        public List<int> IdsAt(int index)
        {
            List<(string, List<int>)> ev = Events();
            return index < ev.Count ? ev[index].Item2 : new List<int>();
        }
    }

    private static string Text(int queueNums) =>
        string.Join(",", Enumerable.Range(0, queueNums));

    private static void Scale(string namesrv, int queueNums)
    {
        var prod = new DefaultMQProducer("PG_QcNet_" + Guid.NewGuid().ToString("N"));
        prod.NamesrvAddr = namesrv;
        prod.Start();
        try
        {
            prod.CreateTopic("TBW102", Topic, queueNums);
        }
        catch (Exception e)
        {
            Console.WriteLine("!! CreateTopic(" + queueNums + ") failed: " + e.Message);
        }
        prod.Shutdown();
    }

    /// <summary>现查队列集合，直到报出 want 个为止；返回等待毫秒数，超时返回 -1。</summary>
    private static long WaitQueueNum(DefaultLitePullConsumer c, string topic, int want,
        int timeoutMillis)
    {
        long waited = 0;
        while (waited < timeoutMillis)
        {
            try
            {
                if (c.FetchMessageQueues(topic).Count == want) return waited;
            }
            catch (Exception)
            {
                // 路由还没更新，继续等
            }
            Thread.Sleep(250);
            waited += 250;
        }
        return -1;
    }

    /// <summary>等监听器记到 want 条回调；返回等待毫秒数，超时返回 -1。</summary>
    private static long WaitEvents(Recorder rec, int want, int timeoutMillis)
    {
        long waited = 0;
        while (waited < timeoutMillis)
        {
            if (rec.Count >= want) return waited;
            Thread.Sleep(100);
            waited += 100;
        }
        return -1;
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        Console.WriteLine(new string('=', 70));
        Console.WriteLine("LitePull queue-change live (C#): namesrv=" + namesrv
            + " topic=" + Topic + " group=" + Group);
        Console.WriteLine(new string('=', 70));

        Scale(namesrv, BaseQueues);

        var c = new DefaultLitePullConsumer(Group);
        c.SetNamesrvAddr(namesrv);
        c.SetInstanceName("lite-qc-live");
        c.SetTopicMetadataCheckIntervalMillis(CheckIntervalMillis);
        Check("L0 检查周期压到 1s 下限",
            c.TopicMetadataCheckIntervalMillis == CheckIntervalMillis,
            c.TopicMetadataCheckIntervalMillis.ToString(CultureInfo.InvariantCulture));
        c.Subscribe(Topic, "*");
        c.Start();
        DateTime loopStart = DateTime.UtcNow;

        long visible = WaitQueueNum(c, Topic, BaseQueues, 30000);
        Check("L0 路由可见（2 个队列）", visible >= 0,
            visible.ToString(CultureInfo.InvariantCulture) + "ms");

        // 运行中注册 ⇒ 立刻记快照 ⇒ 首轮不该回调
        var rec = new Recorder();
        c.RegisterTopicMessageQueueChangeListener(Topic, rec);
        Thread.Sleep(3000);
        Check("L1 队列没动 ⇒ 静默", rec.Count == 0,
            "events=" + rec.Count.ToString(CultureInfo.InvariantCulture));

        int elapsed = (int)(DateTime.UtcNow - loopStart).TotalMilliseconds;
        if (elapsed < FirstDelayBudgetMillis)
        {
            Thread.Sleep(FirstDelayBudgetMillis - elapsed);
        }
        Check("L1b 首查那趟真的跑过 ⇒ 依旧静默", rec.Count == 0,
            "events=" + rec.Count.ToString(CultureInfo.InvariantCulture));

        // ---- L2 扩容 2→4
        Scale(namesrv, ScaledQueues);
        long nsMs = WaitQueueNum(c, Topic, ScaledQueues, 45000);
        Check("L2a nameserver 报出 4 个队列", nsMs >= 0,
            nsMs.ToString(CultureInfo.InvariantCulture) + "ms");
        if (nsMs >= 0)
        {
            long cb = WaitEvents(rec, 1, FreshWindowMillis);
            Check("L2b 比对趟次现查路由（回调紧跟 nameserver，不等 30s 缓存）",
                cb >= 0 && string.Join(",", rec.IdsAt(0)) == Text(ScaledQueues),
                cb.ToString(CultureInfo.InvariantCulture) + "ms, ids="
                + string.Join(",", rec.IdsAt(0)));
        }

        // ---- L3 快照推进
        Thread.Sleep(3000);
        Check("L3 回调后快照推进 ⇒ 不重复回调", rec.Count == 1,
            "events=" + rec.Count.ToString(CultureInfo.InvariantCulture));

        // ---- L4 缩容 4→2
        Scale(namesrv, BaseQueues);
        nsMs = WaitQueueNum(c, Topic, BaseQueues, 45000);
        Check("L4a nameserver 报回 2 个队列", nsMs >= 0,
            nsMs.ToString(CultureInfo.InvariantCulture) + "ms");
        if (nsMs >= 0)
        {
            long cb = WaitEvents(rec, 2, FreshWindowMillis);
            Check("L4b 缩容同样靠现查路由看到",
                cb >= 0 && string.Join(",", rec.IdsAt(1)) == Text(BaseQueues),
                cb.ToString(CultureInfo.InvariantCulture) + "ms, ids="
                + string.Join(",", rec.IdsAt(1)));
        }

        // ---- L5 未知 topic：空队列集算「查不到」
        string raised = "";
        try
        {
            raised = "no exception, queues="
                + c.FetchMessageQueues(Ghost).Count.ToString(CultureInfo.InvariantCulture);
        }
        catch (Exception e)
        {
            raised = e.Message;
        }
        Check("L5 未知 topic 取队列抛「查不到」而不是返回空",
            raised.Contains("Namesrv return empty") || raised.Contains("Can not find"), raised);

        c.Shutdown();
        Console.WriteLine();
        Console.WriteLine("LitePull queue-change live (C#): PASS="
            + _pass.ToString(CultureInfo.InvariantCulture) + " FAIL="
            + _fail.ToString(CultureInfo.InvariantCulture));
        return _fail == 0 ? 0 : 1;
    }
}
