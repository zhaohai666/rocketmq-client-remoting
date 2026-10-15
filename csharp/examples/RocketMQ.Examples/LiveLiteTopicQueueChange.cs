// Real-machine verification of the LitePull consumer's topic queue set change listener.
// Usage: rmq lite-queue-change [namesrv]
//
// Why it must be a real machine: the comparison round asks the nameserver for the
// subscription queue set afresh every round, while the ordinary route cache is only refreshed
// every 30s. A fake nameserver can prove the comparison logic; only a real cluster can prove
// "asking afresh". Here the check interval is pushed down to its 1s floor while the route
// poll stays at the 30s default, and then the topic really is scaled up: within the steady
// state after the first-lookup delay, only one or two check rounds (<=5s) may separate the
// nameserver reporting the new queue count from the listener receiving the callback; reading
// the 30s cache would stretch that window to over half a minute.
//
//   L1  the queues did not move => the listener is not pestered
//   L1b the first-lookup round really ran => still silent (a snapshot taken while running does not count as a change)
//   L2  scale up 2->4: the callback follows a few check rounds after the nameserver accepts it
//   L3  after the callback the snapshot advances => the same queue set does not fire again
//   L4  scale down 4->2: also seen by asking afresh
//   L5  a topic that was never created: failing to get the queues counts as "not found", it does not pretend to be a scale-down to 0 queues
using System.Globalization;

using RocketMQ.Client;
using RocketMQ.Common;

namespace RocketMQ.Examples;

public static class LiveLiteTopicQueueChange
{
    private const int BaseQueues = 2;
    private const int ScaledQueues = 4;
    // The check interval is pushed to its floor, to separate "asking afresh every round" from "eating the 30s cache" in time.
    private const int CheckIntervalMillis = 1000;
    // The background comparison's first-lookup delay of 10s plus slack: after that only the steady state of one round per 1s remains.
    private const int FirstDelayBudgetMillis = 12000;
    // The maximum callback interval allowed after the nameserver sees the change.
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

    /// <summary>Only records callbacks and does not change state; the background thread
    /// calls it too, so it brings its own lock.</summary>
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

    /// <summary>Asks for the queue set afresh until it reports want; returns how many
    /// milliseconds it waited, or -1 on timeout.</summary>
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
     // the route is not updated yet, keep waiting
            }
            Thread.Sleep(250);
            waited += 250;
        }
        return -1;
    }

    /// <summary>Waits until the listener records want callbacks; returns how many
    /// milliseconds it waited, or -1 on timeout.</summary>
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
  Check("L0 the check interval is pushed to the 1s floor",
            c.TopicMetadataCheckIntervalMillis == CheckIntervalMillis,
            c.TopicMetadataCheckIntervalMillis.ToString(CultureInfo.InvariantCulture));
        c.Subscribe(Topic, "*");
        c.Start();
        DateTime loopStart = DateTime.UtcNow;

        long visible = WaitQueueNum(c, Topic, BaseQueues, 30000);
     Check("L0 the route is visible (2 queues)", visible >= 0,
            visible.ToString(CultureInfo.InvariantCulture) + "ms");

        // registering while running => a snapshot is taken at once => the first round must not fire
        var rec = new Recorder();
        c.RegisterTopicMessageQueueChangeListener(Topic, rec);
        Thread.Sleep(3000);
        Check("L1 the queues did not move => silence", rec.Count == 0,
            "events=" + rec.Count.ToString(CultureInfo.InvariantCulture));

        int elapsed = (int)(DateTime.UtcNow - loopStart).TotalMilliseconds;
        if (elapsed < FirstDelayBudgetMillis)
        {
            Thread.Sleep(FirstDelayBudgetMillis - elapsed);
        }
        Check("L1b the first-lookup round really ran => still silent", rec.Count == 0,
            "events=" + rec.Count.ToString(CultureInfo.InvariantCulture));

    // ---- L2 scale up 2->4
        Scale(namesrv, ScaledQueues);
        long nsMs = WaitQueueNum(c, Topic, ScaledQueues, 45000);
     Check("L2a the nameserver reports 4 queues", nsMs >= 0,
            nsMs.ToString(CultureInfo.InvariantCulture) + "ms");
        if (nsMs >= 0)
        {
            long cb = WaitEvents(rec, 1, FreshWindowMillis);
       Check("L2b the comparison round asks for the route afresh (the callback follows the nameserver, it does not wait out the 30s cache)",
                cb >= 0 && string.Join(",", rec.IdsAt(0)) == Text(ScaledQueues),
                cb.ToString(CultureInfo.InvariantCulture) + "ms, ids="
                + string.Join(",", rec.IdsAt(0)));
        }

        // ---- L3 the snapshot advances
        Thread.Sleep(3000);
     Check("L3 after the callback the snapshot advances => no repeat callback", rec.Count == 1,
            "events=" + rec.Count.ToString(CultureInfo.InvariantCulture));

      // ---- L4 scale down 4->2
        Scale(namesrv, BaseQueues);
        nsMs = WaitQueueNum(c, Topic, BaseQueues, 45000);
       Check("L4a the nameserver reports 2 queues again", nsMs >= 0,
            nsMs.ToString(CultureInfo.InvariantCulture) + "ms");
        if (nsMs >= 0)
        {
            long cb = WaitEvents(rec, 2, FreshWindowMillis);
       Check("L4b the scale-down is also seen by asking for the route afresh",
                cb >= 0 && string.Join(",", rec.IdsAt(1)) == Text(BaseQueues),
                cb.ToString(CultureInfo.InvariantCulture) + "ms, ids="
                + string.Join(",", rec.IdsAt(1)));
        }

     // ---- L5 an unknown topic: an empty queue set counts as "not found"
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
        Check("L5 asking for the queues of an unknown topic throws 'not found' rather than returning empty",
            raised.Contains("Namesrv return empty") || raised.Contains("Can not find"), raised);

        c.Shutdown();
        Console.WriteLine();
        Console.WriteLine("LitePull queue-change live (C#): PASS="
            + _pass.ToString(CultureInfo.InvariantCulture) + " FAIL="
            + _fail.ToString(CultureInfo.InvariantCulture));
        return _fail == 0 ? 0 : 1;
    }
}
