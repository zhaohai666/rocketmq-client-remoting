// Prerequisites: NameServer + Broker running, autoCreateTopicEnable=true.
//
//
// ⚠ Three script disciplines (conventions established during the LiveBackPressure live-verification round):
//   ① Topics always carry a stamp, to avoid inheriting the message count from the previous round;
//   ② "Whether it landed" is judged only by the sum of maxOffset-minOffset across queues on the broker side, not by client callbacks;
//   ③ A newly created topic must wait for the broker to register its topicConfig with the namesrv (seconds up to tens of seconds), so all
//      broker-side reconciliations poll until timeout; one failed route read does not count as a failure.
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

    private sealed class Recorder : ISendCallback
    {
        private readonly object _lk = new();
        private readonly List<SendResult> _results = new();
        private readonly List<Exception> _errors = new();

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
            lock (_lk) return _errors.Count == 0 ? string.Empty : _errors[0].Message;
        }

        /// <summary>Type name of the first failure - after #78 the callback receives the Exception itself,
        /// so "which kind of failure" can be asserted without regexing against message text.</summary>
        public string FirstErrorType()
        {
            lock (_lk) return _errors.Count == 0 ? string.Empty : _errors[0].GetType().Name;
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

        public void OnException(Exception error)
        {
            lock (_lk)
            {
                _errors.Add(error);
                LastThreadName = Thread.CurrentThread.Name ?? string.Empty;
            }
        }
    }

    /// <summary>The before hook sleeps for a while: slowing down the "prepare phase" is the only way to prove the caller did not wait for it.
    /// It also records two things: which thread the hook runs on, and how many times before/after each ran.</summary>
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

    /// <summary>Rejects only messages carrying the forbidden tag, and records the CommunicationMode it observed.</summary>
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

    /// <summary>How many messages landed on the broker for this topic in total (sum of maxOffset-minOffset across queues);
    /// returns -1 when the route cannot be read (a new topic registers with the namesrv within seconds).</summary>
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
                // Queue just created, nothing written yet
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

    // ------------------------------------------------- A1 immediate return + thread accounting + read-back
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
            Check("A1 caller returned before the prepare phase (hook sleeps 400ms)",
                callerTook < 200 && hook.Before() <= 1,
                "caller took=" + N(callerTook) + "ms, before ran=" + N(hook.Before()));
            Check("A1 callback fired exactly once with SEND_OK",
                rec.WaitDone(1, 20000) && rec.Ok == 1 && rec.ErrorCount == 0, rec.Summary());
            SendResult r = rec.Results()[0];
            Check("A1 before hook ran on AsyncSenderExecutor_N",
                hook.BeforeThread().StartsWith("AsyncSenderExecutor_", StringComparison.Ordinal),
                "thread name=" + hook.BeforeThread());
            Check("A1 user callback ran on NettyClientPublicExecutor_N",
                rec.LastThreadName.StartsWith("NettyClientPublicExecutor_", StringComparison.Ordinal),
                "thread name=" + rec.LastThreadName);
            Check("A1 before/after each ran once", hook.Before() == 1 && hook.After() == 1,
                "before=" + N(hook.Before()) + " after=" + N(hook.After()));
            Check("A1 broker landed this message", WaitLanded(env, t, 1) == 1);
            // offsetMsgId is broker-assigned; only it can decode the commitLog offset => read back and compare body
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
                    return false;  // commitLog not flushed yet
                }
            }, 15000);
            Check("A1 can read this message back via the offsetMsgId from the callback",
                read && back is not null
                && Encoding.UTF8.GetString(back!.Body) == body,
                "body=" + (back is null ? "<null>" : Encoding.UTF8.GetString(back.Body)));
            Check("A1 queueOffset in the callback matches where it landed",
                r.QueueOffset == env.Admin.MaxOffset(r.MessageQueue) - 1,
                "queueOffset=" + N(r.QueueOffset)
                + " maxOffset=" + N(env.Admin.MaxOffset(r.MessageQueue)));
            // MsgId is the client-supplied UNIQ_KEY (32 hex chars), not the broker's copy
            Check("A1 MsgId is the client UNIQ_KEY and differs from the broker offsetMsgId",
                r.MsgId.Length == 32 && r.MsgId != r.OffsetMsgId,
                "msgId=" + r.MsgId + " offsetMsgId=" + r.OffsetMsgId);
        }
        finally
        {
            p.Shutdown();
        }
    }

    // ------------------------------------------------- A2 concurrency without cross-talk
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
            Check("A2 all 30 concurrent async sends reached a terminal state", allBack,
                "done=" + N(each.Sum(r => r.Done)));
            Check("A2 each send got **exactly one** terminal state (no more, no fewer)",
                each.All(r => r.Done == 1),
                "sends with extra callbacks=" + N(each.Count(r => r.Done > 1)));
            Check("A2 all SEND_OK",
                each.All(r => r.Ok == 1 && r.ErrorCount == 0),
                each[0].Summary());
            Check("A2 exactly 30 messages landed on the broker", WaitLanded(env, t, burst) == burst);
            List<SendResult> rs = each.SelectMany(r => r.Results()).ToList();
            HashSet<string> slots = rs
                .Select(r => r.MessageQueue.BrokerName + "#" + N(r.MessageQueue.QueueId)
                                      + "@" + N(r.QueueOffset))
                .ToHashSet(StringComparer.Ordinal);
            Check("A2 (broker, queueId, queueOffset) slots do not overlap across sends",
                slots.Count == burst, "after dedup=" + N(slots.Count));
            Check("A2 UNIQ_KEY differs for every send",
                rs.Select(r => r.MsgId).Distinct(StringComparer.Ordinal).Count() == burst);
        }
        finally
        {
            p.Shutdown();
        }
    }

    // ------------------------------------------------- A3 pinned send
    private static void A3PinnedQueue(Env env, string t)
    {
        DefaultMQProducer p = MakeProducer(env, "a3");
        var rec = new Recorder();
        try
        {
            // First grow the topic (send one sync message so the broker creates all queues), then pick a queue for a pinned send.
            // ⚠ Before taking the baseline, the warm-up message must already be **readable** on the broker: a just-acked message
            // reaches the consumeQueue with delay; a baseline of 0 turning into 1 at reconciliation would phantom-in one extra message.
            p.Send(Msg(t, "async-a3-warmup"), 5000);
            Check("A3 warm-up message already readable on the broker", WaitLanded(env, t, 1) >= 1,
                "landed=" + N(Landed(env, t)));
            List<MessageQueue> queues = p.FetchPublishMessageQueues(t);
            Check("A3 obtained publish queues", queues.Count > 0,
                "queues=" + N(queues.Count));
            MessageQueue aimed = queues[0];
            long before = QueueLanded(env, aimed);
            long othersBefore = queues.Skip(1).Sum(q => Math.Max(0, QueueLanded(env, q)));
            p.SendAsync(Msg(t, "async-a3-pinned"), rec, 5000, aimed);
            Check("A3 pinned async send reached a terminal state with SEND_OK",
                rec.WaitDone(1, 20000) && rec.Ok == 1, rec.Summary());
            SendResult r = rec.Results()[0];
            Check("A3 result landed on the specified queue",
                r.MessageQueue.BrokerName == aimed.BrokerName
                && r.MessageQueue.QueueId == aimed.QueueId,
                "broker=" + r.MessageQueue.BrokerName + " queueId=" + N(r.MessageQueue.QueueId));
            Check("A3 that queue gained exactly 1 message",
                WaitUntil(() => QueueLanded(env, aimed) == before + 1, 20000),
                "landed=" + N(QueueLanded(env, aimed)) + " before=" + N(before));
            long othersAfter = queues.Skip(1).Sum(q => Math.Max(0, QueueLanded(env, q)));
            Check("A3 no other queue gained any message", othersAfter == othersBefore,
                "other queues " + N(othersBefore) + " -> " + N(othersAfter));
        }
        finally
        {
            p.Shutdown();
        }
    }

    // ------------------------------------------------- A4 interception hook
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
            Check("A4 exception rejected by the hook reached the callback verbatim (type + message preserved)",
                rejected.WaitDone(1, 10000)
                && rejected.FirstError().Contains("tag forbidden is not allowed",
                    StringComparison.Ordinal)
                && rejected.FirstErrorType() == nameof(MQClientException),
                rejected.Summary() + " type=" + rejected.FirstErrorType());
            Check("A4 interception hook observed ASYNC", forbidden.Mode() == CommunicationMode.Async,
                "mode=" + forbidden.Mode());
            // This topic contains nothing except the rejected message => either we read 0 messages, or the route
            // is not registered yet (-1). The route is only auto-created **when the first message lands on the broker**,
            // so "route not readable" is itself evidence that "the broker never received a request".
            long afterReject = Landed(env, t);
            Check("A4 rejected message left no trace on the broker", afterReject <= 0,
                "landed=" + N(afterReject) + " (-1 = route not created yet, i.e. broker received nothing)");

            p.SendAsync(Msg(t, "async-a4-ok"), passed, 5000);
            Check("A4 same producer with a different tag lands normally (rejection did not break the pool)",
                passed.WaitDone(1, 20000) && passed.Ok == 1, passed.Summary());
            Check("A4 exactly 1 message landed on the broker", WaitLanded(env, t, 1) == 1,
                "landed=" + N(Landed(env, t)));
            Check("A4 hook called exactly 2 times (one rejected, one passed)", forbidden.Calls() == 2,
                "calls=" + N(forbidden.Calls()));
        }
        finally
        {
            p.Shutdown();
        }
    }

    // ------------------------------------------------- A5 batch goes through the sync batch kernel
    private static long OthersLanded(Env env, List<MessageQueue> queues)
    {
        long total = 0;
        for (int i = 1; i < queues.Count; i++)
        {
            total += Math.Max(0, QueueLanded(env, queues[i]));
        }

        return total;
    }

    private static void A5BatchAsync(Env env, string t)
    {
        DefaultMQProducer p = MakeProducer(env, "a5");

        // ① A batch of 3: callback fires exactly once with SEND_OK, and 3 messages really landed on the broker.
        //    Trusting the callback alone can be fooled by "callback reports OK but the request never went out" - broker-side reconciliation is mandatory.
        var rec = new Recorder();
        var list = new List<Message>
        {
            Msg(t, "async-a5-0"), Msg(t, "async-a5-1"), Msg(t, "async-a5-2"),
        };
        p.SendBatchAsync(list, rec, 10000);
        Check("A5 batch of 3: callback fired exactly once with SEND_OK",
            rec.WaitDone(1, 20000) && rec.Done == 1 && rec.Ok == 1
                && rec.Results().Count == 1 && rec.Results()[0].MsgId.Length == 32, rec.Summary());
        long landed = WaitLanded(env, t, 3);
        Check("A5 3 messages really landed on the broker (not just the callback saying so)", landed == 3,
            "landed=" + N(landed) + " (-1 = route not created yet)");
        Skip("A5 request code = SEND_BATCH_MESSAGE(320)",
            "live cluster cannot observe outgoing packets; ProducerAsyncTests.BatchAsync proves this in-process");

        SendResult batchResult = rec.Results().Count > 0 ? rec.Results()[0] : new SendResult();
        Check("A5 batch MsgId is the client 32-hex ID, not the broker OffsetMsgId",
            batchResult.MsgId.Length == 32 && !batchResult.MsgId.Contains(',')
                && batchResult.MsgId != batchResult.OffsetMsgId,
            "MsgId=" + batchResult.MsgId + " OffsetMsgId=" + batchResult.OffsetMsgId);
        // The batch response's OffsetMsgId is a comma-separated list returned **per message** (one
        // commitLog offset per sub-message); it is itself evidence that "the batch was split and stored as 3 messages"
        string[] subOffsets = Array.FindAll(
            batchResult.OffsetMsgId.Split(','), s => s.Length > 0);
        Check("A5 broker returned 3 commitLog offsets, one per message (batch really split on landing)",
            subOffsets.Length == 3, "OffsetMsgId=" + batchResult.OffsetMsgId);
        string storedUniq = string.Empty;
        if (subOffsets.Length > 0)
        {
            MessageExt? storedSub = null;
            Check("A5 can read back the sub-message stored on the broker",
                WaitUntil(() =>
                {
                    try
                    {
                        storedSub = env.Admin.ViewMessage(t, subOffsets[0]);
                        return true;
                    }
                    catch (Exception)
                    {
                        return false; // commitLog not flushed yet
                    }
                }, 15000), "offset=" + subOffsets[0]);
            // MessageConst.PropertyUniqClientMessageIdKeyidx == "UNIQ_KEY"
            storedUniq = storedSub?.GetProperty("UNIQ_KEY") ?? string.Empty;
        }

        Check("A5 sub-message stored on the broker carries the client UNIQ_KEY (per-message ID encoded in body)",
            storedUniq.Length == 32, "stored UNIQ_KEY=" + storedUniq);

        // ② Pinned batch: the mq parameter really reached the batch kernel instead of being dropped with a self-chosen queue.
        List<MessageQueue> queues;
        try
        {
            queues = env.Admin.ExamineTopicRoute(t).GetAllMessageQueue(t);
        }
        catch (Exception e)
        {
            Check("A5 pinned batch obtained publish queues", false, e.Message);
            p.Shutdown();
            return;
        }

        if (queues.Count == 0)
        {
            Check("A5 pinned batch obtained publish queues", false, "queues=0");
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
        Check("A5 pinned batch added exactly 3 messages to that queue",
            WaitUntil(() => (aimedAfter = QueueLanded(env, aimed)) == aimedBefore + 3, 20000),
            "that queue " + N(aimedBefore) + " -> " + N(aimedAfter));
        long othersAfter = OthersLanded(env, queues);
        Check("A5 pinned batch landed on the specified queue; no other queue gained messages",
            pinnedOk && landedOnAimed && aimedAfter == aimedBefore + 3 && othersAfter == othersBefore,
            "ok=" + N(pinned.Ok) + " landedOnAimed=" + (landedOnAimed ? "1" : "0")
                + " that queue " + N(aimedBefore) + "->" + N(aimedAfter)
                + " others " + N(othersBefore) + "->" + N(othersAfter));

        var mixed = new Recorder();
        p.SendBatchAsync(
            new List<Message> { Msg(t, "async-a5-mixed-0"), Msg(env.Topic("BatchOther"), "async-a5-mixed-1") },
            mixed, 5000);
        Check("A5 mixed-topic batch reports an error in the callback (local validation not bypassed by the async path)",
            mixed.WaitDone(1, 10000) && mixed.Done == 1 && mixed.Ok == 0
                && mixed.FirstError().Contains("should be the same"), mixed.Summary());

        // ③ Empty batch: same rule - the error goes to the callback; not a single callback is owed.
        var empty = new Recorder();
        p.SendBatchAsync(new List<Message>(), empty, 5000);
        Check("A5 empty batch also gets 'exactly one failure callback', never silently swallowed",
            empty.WaitDone(1, 10000) && empty.Done == 1 && empty.Ok == 0
                && empty.FirstError().Contains("message list is empty"), empty.Summary());

        // ④ Backpressure reserves bytes for the **whole batch**, not "one message": with the byte gate set to the floor (1 MiB), a batch of 2x600 KiB
        //    must be rejected (whole batch 1.2 MiB > 1 MiB); a batch of 2x100 KiB must pass. Counting only the first message
        //    (600 KiB), or not reserving at all, would make both assertions fail in opposite directions.
        p.EnableBackpressureForAsyncMode = true;
        p.BackPressureForAsyncSendNum = 1;
        p.BackPressureForAsyncSendSize = 1024 * 1024;
        var gated = new Recorder();
        p.SendBatchAsync(
            new List<Message> { Msg(t, new string('x', 600 * 1024)), Msg(t, new string('x', 600 * 1024)) },
            gated, 2000);
        Check("A5 whole-batch 1.2MiB rejected by the 1MiB byte gate (callback got TOO_MUCH_REQUEST)",
            gated.WaitDone(1, 15000) && gated.Done == 1 && gated.Ok == 0
                && gated.FirstError().Contains("semaphoreAsyncSize timeout"), gated.Summary());
        // A small batch under the same gate must land normally: proves the rejection is due to whole-batch size, not a blanket batch policy
        var passed = new Recorder();
        p.SendBatchAsync(
            new List<Message> { Msg(t, new string('y', 100 * 1024)), Msg(t, new string('y', 100 * 1024)) },
            passed, 10000);
        Check("A5 a 200KiB batch under the same gate still gets SEND_OK",
            passed.WaitDone(1, 20000) && passed.Done == 1 && passed.Ok == 1, passed.Summary());
        // Permits must be returned intact: missing one means "all async sends time out collectively" after a long run, invisible in single sends.
        int numTotal = p.GetBackPressureForAsyncSendNum();
        int sizeTotal = p.GetBackPressureForAsyncSendSize();
        Check("A5 batch path returned both permits intact (available returns to total, including the gated send)",
            WaitUntil(() => p.SemaphoreAsyncSendNumAvailablePermits == numTotal
                && p.SemaphoreAsyncSendSizeAvailablePermits == sizeTotal, 10000),
            "available count=" + N(p.SemaphoreAsyncSendNumAvailablePermits) + " total=" + N(numTotal)
                + " available bytes=" + N(p.SemaphoreAsyncSendSizeAvailablePermits) + " total=" + N(sizeTotal));
        p.Shutdown();
    }

    // ------------------------------------------------- A6 shutdown drains the in-flight prepare phase
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

            p.Shutdown();
            long landed = WaitLanded(env, t, sends, 40);
            Check("A6 Shutdown drained the queue: every submitted send went out",
                landed == sends, "landed=" + N(landed) + " sends=" + N(sends));
            Check("A6 at most one terminal callback per send", rec.Done <= sends,
                "done=" + N(rec.Done) + " sends=" + N(sends));
        }
        catch (Exception e)
        {
            // Shutdown already called; do not close again in finally
            Check("A6 Shutdown drains the in-flight prepare phase", false, e.Message);
        }

        p = MakeProducer(env, "a6-after");
        var after = new Recorder();
        p.SendAsync(Msg(t, "async-a6-after"), after, 8000);
        Check("A6 closed pool is not reused by another producer (a new producer can still send)",
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
        Console.WriteLine("RocketMQ async send kernel live verify (C#): namesrv="
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
            Check("script overall execution", false, e.Message);
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
                    // cleanup failure does not affect the verdict
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
