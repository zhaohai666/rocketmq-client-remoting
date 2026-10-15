// 220 is the reset instruction the broker pushes to the **consumer side**; the response of
// the admin-side INVOKE_BROKER_TO_RESET_OFFSET(222) call is only a table of "which offset
// each queue was reset to", and what really makes the consumer change its offset is the
// oneway 220 the broker pushes afterwards (Broker2Client.resetOffset:181-238). The broker
// only takes this push path when useServerSideResetOffset=false (with the default true,
// AdminBrokerProcessor:2255-2263 changes the offset server side and notifies not a single
// consumer), so this verification hot-changes that switch to false first (confirming the
// read-back) and restores it when done.
//
// The offline unit tests (tests/RocketMQ.Client.Tests/ResetOffsetTests.cs) pin down both
// shapes of the request body and the local state of "withdraw the queue + generation + 1 +
// the new offset flushed via the withdrawal tail"; the following things only a real cluster
// can prove:
//
//   S1 "a backward reset takes effect at once + in-flight batches are invalidated" -- the
//      offset is reset from 10 back to 3, in three legs of criteria:
//      a) the broker-side committed offset becomes 3 within ~2s: **only** the persist that
//         the reset path brings itself writes it inside the window (the periodic flush has
//         been stretched to 60s), while an implementation that only writes the in-memory
//         table stays put (the broker remains at 10);
//      b) the old batch stuck in the listener (offset 10, fetched before the reset) has its
//         ack invalidated as a whole once released -- the sampling point: after releasing
//         the old batch and while the **first batch** of the new queue is in the listener
//         and not yet acked, the local consumed offset must still be "no record / <=3"; an
//         implementation without the generation gate jumps to 11 here;
//      c) the queue really is rebuilt: every batch of 3..14 is **redelivered exactly
//         once** (the un-acked 11..14 in the old buffer are invalidated too and can only
//         appear as part of the redelivery); one round is asserted per release.
//
//   S2 "forward jump + recovery" -- timestamp=-1 resets to maxOffset: the offset jumps
// straight to 10 and none of 4..9 in between is delivered; afterwards new messages are
// consumed as usual and the offset keeps advancing.
//
// Usage: rmq reset-offset [namesrv] (needs a local 5.5.1 cluster, autoCreateTopicEnable=true)
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveResetOffset
{
    /// <summary>The message count on the single-queue topic: S1 needs 10 (3..14 redelivered
   /// after the reset back to 3), S2 needs 10.</summary>
    private const int Msgs = 10;

    /// <summary>S1's backward target: reset by timestamp between the 3rd and the 4th message
   /// => it hits offset 3.</summary>
    private const long BackTarget = 3;

    private const string SwitchKey = "useServerSideResetOffset";

    private static string _namesrv = "127.0.0.1:9876";

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

    private static string Join(IEnumerable<long> v) => "[" + string.Join(", ", v.Select(N)) + "]";

    private static byte[] Str2Bytes(string s) => Encoding.UTF8.GetBytes(s);

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

  /// <summary>For criteria on "something is about to happen": it samples more densely than
   /// <see cref="WaitUntil"/>.</summary>
    private static bool WaitBefore(Func<bool> pred, int windowMs) => WaitUntil(pred, windowMs, 50);

   /// <summary>The queue key (identical to the consumer's internal OffsetKey): topic +
    /// brokerName + queueId.</summary>
    private static string Key(MessageQueue mq) =>
        mq.Topic + mq.BrokerName + mq.QueueId.ToString(CultureInfo.InvariantCulture);

 /// <summary>Reads back the broker-side (committed offset, maxOffset); Committed = -1 means
 /// no record was found (QUERY_NOT_FOUND), 0 is a legal offset and the two must not be
    /// conflated.</summary>
    private static (long Committed, long Max) Probe(DefaultMQAdminExt admin, string group,
        MessageQueue mq)
    {
        long max;
        try
        {
            max = admin.MaxOffset(mq);
        }
        catch (Exception)
        {
            max = -1;
        }

        long off = -1;
        try
        {
            if (admin.ExamineConsumerOffset(group, mq, out long o)) off = o;
        }
        catch (Exception)
        {
            off = -1;
        }

        return (off, max);
    }

   /// <summary>Gates batch by batch: every batch stops at the gate and the test releases them
  /// one at a time -- so the local state between batches can be sampled.</summary>
    ///
    /// S1's key sampling point (the old batch released, the new queue's first batch not yet
    /// acked) only exists when the batches are "controllable one at a time"; a listener that
    /// releases everything at once mixes 11 (the old batch's ack) with the following
    /// redelivery in a single instant.
   /// The cap only guards against deadlock: the normal path is released batch by batch via
   /// <see cref="Release"/>.</summary>
    private sealed class SteppingListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<List<long>> _batches = new();
        private readonly ManualResetEventSlim _release = new(false);

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                var batch = new List<long>();
                foreach (MessageExt m in msgs) batch.Add(m.QueueOffset);
                _batches.Add(batch);
            }

            _release.Wait(TimeSpan.FromSeconds(90));
       _release.Reset(); // released one batch at a time: reset the gate as soon as this round is released
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public void Release() => _release.Set();

        public int BatchCount()
        {
            lock (_lk) return _batches.Count;
        }

        public List<long> Offsets()
        {
            lock (_lk)
            {
                var all = new List<long>();
                foreach (List<long> b in _batches) all.AddRange(b);
                return all;
            }
        }
    }

    private static bool WaitBatches(SteppingListener l, int n, int timeoutMs) =>
        WaitUntil(() => l.BatchCount() >= n, timeoutMs, 50);

    private sealed class RecordingListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<long> _offsets = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                foreach (MessageExt m in msgs) _offsets.Add(m.QueueOffset);
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public List<long> Offsets()
        {
            lock (_lk) return new List<long>(_offsets);
        }
    }

    private static bool AllBack(DefaultMQAdminExt admin, string group, MessageQueue mq,
        long want) => Probe(admin, group, mq).Committed == want;

    private static string ReadFlag(DefaultMQAdminExt admin, string brokerAddr, string key)
    {
        try
        {
            PropertyMap cfg = admin.GetBrokerConfig(brokerAddr);
            return cfg.TryGetValue(key, out var v) && v != null ? v : "<missing>";
        }
        catch (Exception e)
        {
            return "<" + e.Message + ">";
        }
    }

    private static bool SetServerSideReset(DefaultMQAdminExt admin, string brokerAddr,
        string value)
    {
        try
        {
            var props = new PropertyMap { [SwitchKey] = value };
            admin.UpdateBrokerConfig(brokerAddr, props);
        }
        catch (Exception e)
        {
            Console.WriteLine("  [diag] updateBrokerConfig(" + SwitchKey + "=" + value
                + ") failed: " + e.Message);
            return false;
        }

        Thread.Sleep(1000);
        return ReadFlag(admin, brokerAddr, SwitchKey) == value;
    }

    public static int Run(string[] args)
    {
        if (args.Length > 0 && args[0].Trim().Length > 0) _namesrv = args[0].Trim();
        string stamp = (NowMs() % 1000000).ToString(CultureInfo.InvariantCulture);
        string backTopic = "ResetOffsetBack_" + stamp;
        string skipTopic = "ResetOffsetSkip_" + stamp;
        string backGroup = "GID_ResetOffsetBack_" + stamp;
        string skipGroup = "GID_ResetOffsetSkip_" + stamp;
        Console.WriteLine("namesrv=" + _namesrv + " topic1=" + backTopic + " topic2=" + skipTopic);

        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_namesrv);
        admin.SetTimeoutMillis(10000);
        admin.Start();
        string brokerAddr;
        try
        {
            ClusterInfo cluster = admin.FetchBrokerClusterInfo();
            List<string> addrs = cluster.GetBrokerAddrs();
            if (addrs.Count == 0)
            {
         Check("the cluster liveness probe", false, "the nameServer has no broker registered");
                admin.Shutdown();
                return Report();
            }

            brokerAddr = addrs[0];
        }
        catch (Exception e)
        {
    Check("the cluster liveness probe", false, "fetchBrokerClusterInfo: " + e.Message);
            admin.Shutdown();
            return Report();
        }

        Check("the cluster liveness probe", true, "broker=" + brokerAddr);

        var setup = new MQClientInstance("rosetup-" + stamp, new List<string> { _namesrv });
        setup.Start();
        var producer = new DefaultMQProducer("ResetOffsetLive_pg_" + stamp)
        {
            NamesrvAddr = _namesrv,
            InstanceName = "reset-offset-live-" + stamp,
        };

        try
        {
      // Hot-changing the switch: useServerSideResetOffset=false (the precondition of the
    // 220 push path)
         Check("hot-changing the switch: useServerSideResetOffset=false (the precondition of the 220 push path)",
                SetServerSideReset(admin, brokerAddr, "false"),
      "read-back " + SwitchKey + "=" + ReadFlag(admin, brokerAddr, SwitchKey));

       // Explicitly creating the topic with 1 queue: S1's redelivery sequence (3..14) and
    // S2's forward jump both need a countable queue count
            setup.CreateTopicInRoute(backTopic, 1, 1);
            setup.CreateTopicInRoute(skipTopic, 1, 1);
            producer.Start();
            Thread.Sleep(3000);

            ScenarioBackwardReset(producer, admin, brokerAddr, backTopic, backGroup, stamp);
            ScenarioForwardSkip(producer, admin, brokerAddr, skipTopic, skipGroup, stamp);
        }
        catch (Exception e)
        {
    Check("an unexpected exception occurred during the integration", false, e.ToString());
        }
        finally
        {
            try
            {
                producer.Shutdown();
            }
            catch (Exception)
            {
           // a cleanup failure does not mask the main assertion
            }

            foreach (string t in new[] { backTopic, skipTopic })
            {
                try
                {
                    admin.DeleteTopic(t);
                }
                catch (Exception e)
                {
                    Console.WriteLine("  [WARN] deleteTopic(" + t + ") failed: " + e.Message);
                }
            }

            foreach (string g in new[] { backGroup, skipGroup })
            {
                try
                {
                    admin.DeleteSubscriptionGroup(brokerAddr, g, true);
                }
                catch (Exception e)
                {
                    Console.WriteLine("  [WARN] deleteGroup(" + g + ") failed: " + e.Message);
                }
            }

            try
            {
       Check("restoring useServerSideResetOffset=true",
                    SetServerSideReset(admin, brokerAddr, "true"),
 "read-back " + SwitchKey + "=" + ReadFlag(admin, brokerAddr, SwitchKey));
            }
            catch (Exception e)
            {
           Check("restoring useServerSideResetOffset=true", false, e.Message);
            }

            try
            {
                setup.Shutdown();
            }
            catch (Exception)
            {
     // a cleanup failure does not mask the main assertion
            }

            try
            {
                admin.Shutdown();
            }
            catch (Exception)
            {
   // a cleanup failure does not mask the main assertion
            }
        }

        return Report();
    }

    private static void ScenarioBackwardReset(DefaultMQProducer producer, DefaultMQAdminExt admin,
        string brokerAddr, string topic, string group, string stamp)
    {
        Console.WriteLine("=== S1 the backward reset (10 -> 3): flushed at once + the in-flight batch invalidated as a whole ===");

   // Precondition: first use an ordinary listener to make the broker-side offset 10 (the
  // reset requires the group to have a record on the broker)
        var seed = new RecordingListener();
        var c0 = new DefaultMQPushConsumer(group)
        {
            InstanceName = "reset-offset-back-seed-" + stamp,
            ConsumeMessageBatchMaxSize = 1,
            ConsumeFromWhere = RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        c0.SetNamesrvAddr(_namesrv);
        c0.SetMessageListener(seed);
        c0.Subscribe(topic, "*");
        c0.Start();

        MessageQueue mq;
        try
        {
            List<MessageQueue> queues = WaitForSingleQueue(c0, topic, "S1");
            if (queues.Count != 1) return;
            mq = queues[0];

            long mid = 0;
            for (int i = 0; i < Msgs; ++i)
            {
                producer.Send(new Message(topic, Str2Bytes("rb-" + N(i))));
                if (i == 2)
                {
                    mid = NowMs();
      // Push message 3 (offset 3) apart in storage time from the first three, so that the
      // later timestamp-based reset reliably hits 3
                    Thread.Sleep(2000);
                }
            }

            bool seeded = WaitUntil(() => seed.Offsets().Count == Msgs, 30000, 200);
   Check("S1-precondition: the consumer consumed " + N(Msgs) + " messages", seeded,
                "arrivals=" + Join(seed.Offsets()));

            bool primed = WaitUntil(() => AllBack(admin, group, mq, Msgs), 20000, 500);
     Check("S1-precondition: the broker-side offset was flushed periodically to " + N(Msgs) + " (the reset requires the group to have a record on the broker)",
                primed, "committed=" + N(Probe(admin, group, mq).Committed));
            c0.Shutdown();
            Thread.Sleep(1000);

        // The main consumer: gated batch by batch + a 60s periodic flush => the only path
        // that can change the broker-side offset inside the window is the reset's own persist
            var listener = new SteppingListener();
            var c = new DefaultMQPushConsumer(group)
            {
                InstanceName = "reset-offset-back-" + stamp,
                ConsumeMessageBatchMaxSize = 1,
                PersistConsumerOffsetIntervalMillis = 60000,
                ConsumeFromWhere =
                    RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset,
            };
            c.SetNamesrvAddr(_namesrv);
            c.SetMessageListener(listener);
            c.Subscribe(topic, "*");
            long tStart = NowMs();
            c.Start();

            try
            {
                Thread.Sleep(2000);
                for (int i = Msgs; i < Msgs + 5; ++i)
                {
                    producer.Send(new Message(topic, Str2Bytes("rb-" + N(i))));
                }

                string key = Key(mq);
                int pending = -1;
                bool arranged = WaitUntil(() =>
                {
                    pending = c.PendingForTest(key).Count;
                    return listener.BatchCount() >= 1 && pending == 4;
                }, 30000, 50);
                List<long> arr = listener.Offsets();
            Check("S1-window ready: 1 in flight (offset 10 stuck in the listener) + 4 left in the buffer",
                    arranged && arr.Count > 0 && arr[0] == Msgs,
                    "arrivals=" + Join(arr) + " pending=" + N(pending));
                if (!arranged) return;

                WaitUntil(() => NowMs() - tStart > 12000, 15000, 200);
     Check("S1-the first periodic flush has passed (no more periodic writes for the next 60s)",
                    NowMs() - tStart > 12000 && AllBack(admin, group, mq, Msgs),
                    "committed=" + N(Probe(admin, group, mq).Committed));

          // Reset by timestamp to 3: the mid lands between the 3rd and the 4th message =>
          // getOffsetInQueueByTime hits 3, and 3 < consumerOffset(10) with isForce=true =>
          // the broker pushes {mq: 3}
                long resetTs = mid + 500;
                long t0 = NowMs();
                SortedDictionary<MessageQueue, long> table =
                    admin.ResetOffsetByTimestamp(topic, group, resetTs, true);
  Check("S1-the target offset in the 222 response is exactly 3",
                    table.Count == 1 && table.Values.First() == BackTarget,
                    "table={" + string.Join(", ", table.Select(kv => "q" + N(kv.Key.QueueId)
                        + ":" + N(kv.Value))) + "}");

                bool in2s = WaitBefore(() => AllBack(admin, group, mq, BackTarget), 2000);
     Check("S1-the broker-side offset becomes 3 within ~2s (the persist the reset path brings itself, periodic flush = 60s)", in2s,
                    "elapsed=" + ((NowMs() - t0) / 1000.0).ToString("F1", CultureInfo.InvariantCulture)
                    + "s committed=" + N(Probe(admin, group, mq).Committed));

                long? off = c.ConsumeOffsetForTest(key);
         Check("S1-after the reset there is no old offset in the local table (the old offset is removed; the new one goes out via the withdrawal tail)",
                    off == null, "local_offset=" + (off.HasValue ? N(off.Value) : "None")
                    + " epoch=" + N(c.QueueEpochForTest(key)));

        // Release the old batch: its ack belongs to a withdrawn ProcessQueue and must be invalidated
                listener.Release();
                bool second = WaitBatches(listener, 2, 20000);
                off = c.ConsumeOffsetForTest(key);
         Check("S1-the old batch's ack is invalidated (after releasing, while the new queue's first batch at offset 3 is in flight, the local offset has still not passed 3)",
                    second && (off == null || off.Value <= BackTarget),
                    "local_offset=" + (off.HasValue ? N(off.Value) : "None")
                    + " arrivals=" + Join(listener.Offsets()));

        // Release batch by batch to walk the redelivery: each of 3..14 exactly once (11..14
        // in the old buffer were invalidated too and can only appear as redeliveries)
                var expected = new List<long> { Msgs };
                for (long i = BackTarget; i < Msgs + 5; ++i) expected.Add(i);
                for (int n = 2; n <= expected.Count; ++n)
                {
                    listener.Release();
                    if (n < expected.Count) WaitBatches(listener, n + 1, 15000);
                }

                arr = listener.Offsets();
        Check("S1-the queue really is rebuilt: the redelivery sequence is " + Join(expected), arr.SequenceEqual(expected),
                    "arrivals=" + Join(arr));
                WaitUntil(() => c.ConsumeOffsetForTest(key) == Msgs + 5, 10000, 50);

      Check("S1-only the reset wrote to the broker inside the window (the offset is still 3, periodic flush = 60s)",
                    AllBack(admin, group, mq, BackTarget),
                    "committed=" + N(Probe(admin, group, mq).Committed));

         // Shutdown flushes synchronously once: the redelivered ack is the final value
        // (15 = the last message 14 plus 1)
                c.Shutdown();
                bool advanced = WaitUntil(() => AllBack(admin, group, mq, Msgs + 5), 15000, 500);
       Check("S1-the shutdown flush writes the redelivered ack back to the broker (the offset advances to 15)", advanced,
                    "committed=" + N(Probe(admin, group, mq).Committed));
            }
            finally
            {
                listener.Release();
                try
                {
                    c.Shutdown();
                }
                catch (Exception)
                {
      // a cleanup failure does not mask the main assertion
                }
            }
        }
        finally
        {
            try
            {
                c0.Shutdown();
            }
            catch (Exception)
            {
        // a cleanup failure does not mask the main assertion
            }
        }
    }

    private static void ScenarioForwardSkip(DefaultMQProducer producer, DefaultMQAdminExt admin,
        string brokerAddr, string topic, string group, string stamp)
    {
        Console.WriteLine("=== S2 the forward reset (timestamp=-1 -> maxOffset) does not redeliver + new messages are consumed as usual ===");

        var listener = new SteppingListener();
        // The periodic flush uses the default here: this scenario first relies on the first
        // hop (10s after start) to give the group a record on the broker
        // (Broker2Client.resetOffset replies SYSTEM_ERROR straight away for a group whose
        // queryOffset==-1), and then waits out a >5s window to do the reset -- the reset's
        // own persist and the periodic write are not at the same instant, so the criteria
        // stay clean.
        var c = new DefaultMQPushConsumer(group)
        {
            InstanceName = "reset-offset-skip-" + stamp,
            ConsumeMessageBatchMaxSize = 1,
            ConsumeFromWhere = RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        c.SetNamesrvAddr(_namesrv);
        c.SetMessageListener(listener);
        c.Subscribe(topic, "*");
        long tStart = NowMs();
        c.Start();

        try
        {
            List<MessageQueue> queues = WaitForSingleQueue(c, topic, "S2");
            if (queues.Count != 1) return;
            MessageQueue mq = queues[0];

            Thread.Sleep(2000);
            for (int i = 0; i < Msgs; ++i)
            {
                producer.Send(new Message(topic, Str2Bytes("rs-" + N(i))));
            }

      // Release the first 3 messages batch by batch: the 4th (offset 3) stays in the
    // listener as the "in-flight batch"
            for (int n = 1; n <= 4; ++n)
            {
                if (!WaitBatches(listener, n, 30000)) break;
                if (n < 4) listener.Release();
            }

            List<long> arr = listener.Offsets();
            var seedArrivals = new List<long> { 0, 1, 2, 3 };
            bool arranged = listener.BatchCount() >= 4 && arr.SequenceEqual(seedArrivals);
            Check("S2-window ready: the first 3 are acked and the 4th is stuck in the listener", arranged,
                "arrivals=" + Join(arr));
            if (!arranged) return;

   // Wait for the first-hop flush to create the group's record (the local consumed offset 3
    // = the acks of the first three) and leave a >5s quiet window
            bool primed = WaitUntil(
                () => NowMs() - tStart > 12000 && AllBack(admin, group, mq, BackTarget), 20000, 500);
   Check("S2-precondition: the group has a record on the broker (q0:3) and the next periodic write is still beyond 5s", primed,
                "committed=" + N(Probe(admin, group, mq).Committed));

            long t0 = NowMs();
            SortedDictionary<MessageQueue, long> table =
                admin.ResetOffsetByTimestamp(topic, group, -1, true);
            Check("S2-the target offset in the 222 response is maxOffset(10)",
                table.Count == 1 && table.Values.First() == Msgs,
                "table={" + string.Join(", ", table.Select(kv => "q" + N(kv.Key.QueueId) + ":"
                    + N(kv.Value))) + "}");

            bool in2s = WaitBefore(() => AllBack(admin, group, mq, Msgs), 2000);
    Check("S2-the broker-side offset jumps forward to 10 within ~2s (the persist the reset path brings itself)", in2s,
                "elapsed=" + ((NowMs() - t0) / 1000.0).ToString("F1", CultureInfo.InvariantCulture)
                + "s committed=" + N(Probe(admin, group, mq).Committed));

            listener.Release();
            Thread.Sleep(3000);
            arr = listener.Offsets();
      Check("S2-none of the skipped 4..9 is delivered (the in-flight one's ack is invalidated too)",
                arr.SequenceEqual(seedArrivals), "arrivals=" + Join(arr));

            producer.Send(new Message(topic, Str2Bytes("rs-new")));
            bool gotNew = WaitBatches(listener, 5, 20000);
            listener.Release();
            arr = listener.Offsets();
            var wantArrivals = new List<long> { 0, 1, 2, 3, Msgs };
      Check("S2-the rebuilt queue resumes from the tail (the new message at offset 10 is delivered normally)",
                gotNew && arr.SequenceEqual(wantArrivals), "arrivals=" + Join(arr));

        // Release() only lets the listener return; the ack is committed by the consume thread
    // afterwards. Shutting down without waiting for the local offset to really reach 11
        // means the shutdown persist can run before the ack (writing back 10 still) -- that
    // is an assertion race, not a semantic problem.
            string key = Key(mq);
            WaitUntil(() => c.ConsumeOffsetForTest(key) == Msgs + 1, 10000, 50);
            c.Shutdown();
            bool advanced = WaitUntil(() => AllBack(admin, group, mq, Msgs + 1), 15000, 500);
    Check("S2-the shutdown flush writes the new message's ack back to the broker (the offset advances to 11)", advanced,
                "committed=" + N(Probe(admin, group, mq).Committed));
        }
        finally
        {
            listener.Release();
            try
            {
                c.Shutdown();
            }
            catch (Exception)
            {
     // a cleanup failure does not mask the main assertion
            }
        }
    }

    private static List<MessageQueue> WaitForSingleQueue(DefaultMQPushConsumer c, string topic,
        string tag)
    {
        List<MessageQueue> queues = new();
        bool one = WaitUntil(() =>
        {
            try
            {
                queues = c.FetchSubscribeMessageQueues(topic);
            }
            catch (Exception)
            {
                return false;
            }

            return queues.Count == 1;
        }, 20000);
 Check(tag + "-the topic has exactly 1 queue", one, "queues=" + N(queues.Count));
        return queues;
    }

    private static int Report()
    {
        Console.WriteLine();
     Console.WriteLine("=== result: " + N(_pass) + " PASS / " + N(_fail) + " FAIL ===");
        return _fail == 0 ? 0 : 1;
    }
}
