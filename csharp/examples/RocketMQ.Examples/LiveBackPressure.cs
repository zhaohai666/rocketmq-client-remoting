// Prerequisite: a NameServer + Broker are up, with autoCreateTopicEnable=true.
//
//
// Three script disciplines (learned from real-machine integration):
//   (1) topics always carry a Stamp -- several rounds run **one after another** against the
// same broker and a fixed name would inherit the previous round's message count;
//   (2) "a rejected send did not even put a request on the wire" can only be judged from the
//      broker-side stored count (the sum of maxOffset-minOffset over the queues); looking at
//      the client callback alone is fooled by an implementation whose "callback reports an
//      error but the request went out anyway";
//   (3) a newly created topic has to wait for the broker to incrementally register its
//      topicConfig with the namesrv (seconds to tens of seconds), so every broker-side
//      reconciliation **polls until the timeout** and one failed route read does not count
//      as a failure.
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting;
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

    /// <summary>A thread-safe callback record (success count / done count / first
   /// error).</summary>
    private sealed class Recorder : ISendCallback
    {
        private readonly object _lk = new();
        private readonly List<Exception> _errors = new();
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

        public void OnException(Exception error)
        {
            lock (_lk)
            {
                Done++;
                _errors.Add(error);
                if (_firstError.Length == 0) _firstError = error.Message;
            }
        }

        public int ErrorCount
        {
            get { lock (_lk) return _errors.Count; }
        }

        public bool AllErrorsContain(string needle, int expected)
        {
            lock (_lk)
            {
                if (_errors.Count != expected || Done != expected) return false;
                return _errors.All(e => e.Message.Contains(needle, StringComparison.Ordinal));
            }
        }

        public bool AllErrorsAre(Type type, int expected)
        {
            lock (_lk)
            {
                return _errors.Count == expected && _errors.All(e => e.GetType() == type);
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
    /// Sleeps for a while inside SendMessageBefore. The permit is taken **when passing the
  /// gate** and returned at the **end of the chain**, so the cleanest way to hold one in
    /// flight is to slow the chain itself down: blocking in the user callback does not hold
    /// a permit (the return happens one step before the result is handed to the user).
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

  // The hook has to be attached before Start(): SlowHook relies on slowing beforeSend to
  // stretch out the time window
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
        // Join(0) means "wait forever" in C# rather than "return at once", so the remaining time gets at least 1ms
        foreach (Thread th in threads) th.Join((int)Math.Max(1L, deadline - NowMs()));
        int stuck = threads.Count(t => t.IsAlive);
        if (stuck > 0)
        {
      Check("*  the sending threads did not wedge", false, stuck + " still alive");
        }
    }

    /// <summary>
   /// How many messages this topic has stored on the broker in total (the sum of
   /// maxOffset-minOffset over the queues); returns -1 when the route cannot be read.
   /// This is the only hard evidence for "a rejected send did not even put a request on the wire".
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
      return -1;  // the route is not registered yet
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
            // that queue was just created and nothing has been written to it yet
            }
        }

        return total;
    }

    /// <summary>Waits until at least expected messages appear on the broker and returns the
  /// last reading (registering a new topic with the namesrv takes seconds).</summary>
    private static long WaitLanded(Env env, string topic, long expected, int seconds = 30)
    {
        long deadline = NowMs() + seconds * 1000L;
        long landed = LandedCount(env.Admin, topic);
        while (landed < expected && NowMs() < deadline)
        {
            Thread.Sleep(500);
            landed = LandedCount(env.Admin, topic);
        }

    if (landed < 0) LandedCount(env.Admin, topic);  // only log it again when it really cannot be read, to ease diagnosis
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
    // use the local defaults when the cluster info cannot be obtained
        }

        return "127.0.0.1:10911";
    }

    // ---------------------------------------------------------------- B1 the default capacity leaks no quota
    private static void B1DefaultCapacityNoLeak(Env env, string t)
    {
        DefaultMQProducer p = MakeProducer(env, "b1", true);
        var rec = new Recorder();
        try
        {
            List<Thread> threads = SpawnSenders(p, t,
                i => Encoding.UTF8.GetBytes("b1-" + N(i) + "-" + new string('x', 120)), 40, rec, 5000);
            JoinAll(threads);
        Check("B1 all 40 async sends ran their callback", WaitUntil(() => rec.Done >= 40, 15000), rec.Summary());
      Check("B1 all SEND_OK", rec.Ok == 40 && rec.ErrorCount == 0, rec.Summary());
            long landed = WaitLanded(env, t, 40);
        Check("B1 exactly 40 landed on the broker", landed == 40, "landed=" + N(landed));
        Check("B1 the count permits are fully returned", NumPermits(p) == 1024, "available=" + N(NumPermits(p)));
        Check("B1 the byte permits are fully returned", SizePermits(p) == 100L * 1024 * 1024,
                "available=" + N(SizePermits(p)));
        }
        finally
        {
            p.Shutdown();
        }
    }

    // ------------------------------------------------- B2/B3 the count gate: rejection, reconciliation, growth
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
    Check("B2 the free count is 0 once in flight is saturated", WaitUntil(() => NumPermits(p) == 0, 3000),
                "available=" + N(NumPermits(p)));

      // the 11th and 12th: the budget is only 150ms, not enough to wait for a permit
            long began = NowMs();
            List<Thread> rejects = SpawnSenders(p, t, _ => Encoding.UTF8.GetBytes("rejected"),
                2, rejected, 150);
            JoinAll(rejects, 10.0);
            long waited = NowMs() - began;
         Check("B2 the gate waits until the budget is exhausted before reporting (it is not a glance-and-reject)", waited >= 130,
            "the caller waited " + N(waited) + "ms");
            Check("B2 the two over-limit callbacks are TooMuchRequest with the text agreed by the protocol",
                rejected.AllErrorsContain("send message tryAcquire semaphoreAsyncNum timeout", 2),
                rejected.Summary());
  Check("B2 the exception type of the over-limit callback is RemotingTooMuchRequestException",
                rejected.AllErrorsAre(typeof(RemotingTooMuchRequestException), 2),
                rejected.Summary());

            JoinAll(holders, 25.0);
         Check("B2 the 10 in-flight ones were all sent",
                WaitUntil(() => held.Ok == (int)FairSemaphore.MinAsyncSendNum, 20000),
                held.Summary());
            long landed2 = WaitLanded(env, t, FairSemaphore.MinAsyncSendNum);
          Check("B2 the two rejected ones left nothing on the broker (not even a request went out)",
                landed2 == FairSemaphore.MinAsyncSendNum,
         "landed=" + N(landed2) + " expected " + N(FairSemaphore.MinAsyncSendNum));
            Check("B2 the count permits are back to 10 once everything landed",
                WaitUntil(() => NumPermits(p) == FairSemaphore.MinAsyncSendNum, 20000),
                "available=" + N(NumPermits(p)));

          // B3: saturate 10 in flight again and raise the capacity to 12 -- whoever is stuck
      // at the gate should be woken up.
   // This round sleeps the hook for 2s: the time the in-flight slot is held must be far
  // larger than the overhead of "start a thread + poll to confirm + start the waiter",
    // otherwise it has already been returned before the check finishes the whole round.
            slow.SetMillis(2000);
            var round2 = new Recorder();
            List<Thread> holders2 = SpawnSenders(p, t, _ => Encoding.UTF8.GetBytes("held2"),
                (int)FairSemaphore.MinAsyncSendNum, round2, 8000);
   Check("B3 the second round saturates in flight just the same", WaitUntil(() => NumPermits(p) == 0, 3000),
                "available=" + N(NumPermits(p)));

            var woken = new Recorder();
         // cross-thread visibility: bool has no Volatile.Read overload, so int + Interlocked
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
        Check("B3 the caller really is stuck at the gate before the growth", !passedGate && NumPermits(p) == 0,
            "the caller passed the gate=" + (passedGate ? "yes" : "no") + " available=" + N(NumPermits(p)));
            p.BackPressureForAsyncSendNum = (int)(FairSemaphore.MinAsyncSendNum + 2);
            waiter.Join(15000);
         // waking the calling thread counts as "passing the gate", but the result only comes
       // back to the callback after the whole chain has run (the hook sleeps 2s this round),
        // so what is waited on here is the callback, not the thread exiting.
            Check("B3 the growth woke the senders stuck at the gate and they went out",
                !waiter.IsAlive && WaitUntil(() => woken.Ok >= 1, 20000), woken.Summary());
            JoinAll(holders2, 25.0);
            Check("B3 the free permits = the new capacity 12 once everything is returned",
                WaitUntil(() => NumPermits(p) == FairSemaphore.MinAsyncSendNum + 2, 25000),
                "available=" + N(NumPermits(p)));
            long expect3 = 2 * FairSemaphore.MinAsyncSendNum + 1;
            long landed3 = WaitLanded(env, t, expect3);
        Check("B3 the broker's total = both rounds in flight + the one that was woken", landed3 == expect3,
    "landed=" + N(landed3) + " expected " + N(expect3));
        }
        finally
        {
            p.Shutdown();
        }
    }

      // ------------------------------------------------- B4 the byte gate (a 1M floor + 600KB)
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
  Check("B4 the in-flight byte permits are exactly the body length taken off",
                WaitUntil(() => SizePermits(p) == wantInFlight, 3000),
     "available=" + N(SizePermits(p)) + " expected " + N(wantInFlight));
            var second = new Thread(() => p.SendAsync(new Message(t, big), two, 150))
            {
                IsBackground = true,
            };
            second.Start();
            second.Join(10000);
     Check("B4 the second 600KB one cannot pass the byte gate, and the text is verbatim as agreed by the protocol",
                two.AllErrorsContain("send message tryAcquire semaphoreAsyncSize timeout", 1),
                two.Summary());
      Check("B4 the exception type of the byte gate rejection is RemotingTooMuchRequestException",
                two.AllErrorsAre(typeof(RemotingTooMuchRequestException), 1), two.Summary());
        Check("B4 the count permits are already back when the byte gate is not passed",
                WaitUntil(() => NumPermits(p) == FairSemaphore.MinAsyncSendNum, 3000),
                "available=" + N(NumPermits(p)));
            first.Join(25000);
        Check("B4 the first one lands normally", WaitUntil(() => one.Done >= 1, 20000), one.Summary());
            long landed = WaitLanded(env, t, 1);
          Check("B4 only the first is on the broker (the rejected one left no trace)", landed == 1, "landed=" + N(landed));
            Check("B4 everything returned: both the count and the bytes are back to the configured amount",
                NumPermits(p) == FairSemaphore.MinAsyncSendNum
                && SizePermits(p) == FairSemaphore.MinAsyncSendSize,
                "num=" + N(NumPermits(p)) + " size=" + N(SizePermits(p)));
        }
        finally
        {
            p.Shutdown();
        }
    }

      // ------------------------------------------------- B5 with backpressure off there is no throttling
    private static void B5GateOff(Env env, string t)
    {
        DefaultMQProducer p = MakeProducer(env, "b5", false, FairSemaphore.MinAsyncSendNum,
            FairSemaphore.MinAsyncSendSize, new SlowHook(200));
        var rec = new Recorder();
        try
        {
            // every third one is 300KB: with backpressure off even the byte gate is not looked at, while with it on this must throttle
            List<Thread> threads = SpawnSenders(p, t,
                i => i % 3 == 0 ? Encoding.UTF8.GetBytes(new string('x', 300 * 1024))
                    : Encoding.UTF8.GetBytes("small"),
                30, rec, 8000);
            JoinAll(threads);
  Check("B5 all 30 concurrent sends succeed with backpressure off", WaitUntil(() => rec.Done >= 30, 25000),
                rec.Summary());
            Check("B5 all SEND_OK", rec.Ok == 30 && rec.ErrorCount == 0, rec.Summary());
            long landed = WaitLanded(env, t, 30);
            Check("B5 all 30 are on the broker", landed == 30, "landed=" + N(landed));
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
        Console.WriteLine("RocketMQ async back-pressure live verify (C#): namesrv="
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
        Check("running the script as a whole", false, e.Message);
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
          // a cleanup failure does not affect the conclusion
            }
        }

        env.Admin.Shutdown();

        Console.WriteLine(new string('#', 60));
        Console.WriteLine("PASS=" + N(_pass) + " FAIL=" + N(_fail));
        foreach (string name in Failed) Console.WriteLine("  FAILED: " + name);
        return _fail == 0 ? 0 : 1;
    }
}
