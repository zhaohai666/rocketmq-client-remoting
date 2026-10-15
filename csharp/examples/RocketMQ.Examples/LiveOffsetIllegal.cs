// The offline unit tests (tests/RocketMQ.Client.Tests/OffsetIllegalRecoverTests.cs, 13
// cases) can only pin how the local state is cleared, which ack is invalidated and when the
// freeze lifts; the following two things only a real cluster can prove:
//
//   S1 "dropping the queue" -- when the broker judges the offset illegal, the messages on
//       that queue that were **already fetched but not yet consumed/acked** must be
//       invalidated as a whole. The approach: the listener blocks on the first message (1 in
//       flight, 2 in the buffer), then resetOffsetByQueueId resets the server-side offset to 3
//       (the next pull gets short-circuited by PullMessageProcessor into OFFSET_RESET => the
//       client sees PullStatus.OffsetIllegal and the corrected value is in the response
//       header's nextBeginOffset). Before the fix: messages 1 and 2 in the buffer are still
//       delivered as usual; after the fix: only the in-flight message 0 is left, and its ack is
//       invalidated because the queue was dropped. Finally message 4 is sent to verify that
//       the rebuilt queue resumes from the corrected offset and the freeze was lifted by the
//       rebuild (the new message's ack lets the broker-side offset advance to 4).
//
//   S2 "flush immediately" -- the corrected offset must be pushed to the broker at once, not
//       left to the periodic flush. The approach: exploit the existing non-atomic behaviour of
//       the two resetOffsetByQueueId RPCs (the 1st commitOffset has no range check and lands
//       first, the 2nd is rejected by resetOffsetInner with "Target offset N not in consume
//       queue range") to make the broker's committed offset the illegal value 103, then let a
//       new consumer with persistConsumerOffsetInterval=60000 pull from 103. The only path
//       inside the window that can write 103 back to 3 (maxOffset) is the persist the
//       correction branch brings itself, with zero deliveries the whole way.
//
//
// Usage: rmq offset-illegal [namesrv] (needs a local 5.5.1 cluster, autoCreateTopicEnable=true)
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveOffsetIllegal
{
    /// <summary>The message count on the single-queue topic: 1 in flight + 2 in the buffer,
   /// which is what makes S1's window hold.</summary>
    private const int Msgs = 3;

    /// <summary>S2's illegal offset: far past that queue's maxOffset(=3), but still inside
   /// the broker's int range.</summary>
    private const long IllegalTarget = 103;

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

    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 1000)
    {
        long deadline = NowMs() + timeoutMs;
        while (NowMs() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(intervalMs);
        }

        return pred();
    }

  /// <summary>The queue key (identical to the consumer's internal OffsetKey): topic +
    /// brokerName + queueId.</summary>
    private static string Key(MessageQueue mq) =>
        mq.Topic + mq.BrokerName + mq.QueueId.ToString(CultureInfo.InvariantCulture);

 /// <summary>Reads back the broker-side (committed offset, maxOffset); committed = -1 means
 /// no record was found (QUERY_NOT_FOUND), 0 is a legal offset and the two must not be
    /// conflated.</summary>
    private static (long Committed, long Max) Probe(DefaultMQAdminExt admin, string group,
        MessageQueue mq)
    {
        long max = -1;
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

  /// <summary>The 1st batch is blocked at the gate (keeping up the "1 in flight + the rest in
    /// the buffer" window), and returns success as usual once released.
    /// The cap only guards against deadlock: the normal path is released explicitly by
 /// <see cref="Release"/> and must be later than the correction -- whether the in-flight
 /// batch's ack is invalidated is exactly what S1 has to prove.</summary>
    private sealed class GatedListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<long> _offsets = new();
        private readonly ManualResetEventSlim _release = new(false);

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                foreach (MessageExt m in msgs) _offsets.Add(m.QueueOffset);
            }

            _release.Wait(TimeSpan.FromSeconds(90));
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public void Release() => _release.Set();

        public List<long> Offsets()
        {
            lock (_lk) return new List<long>(_offsets);
        }
    }

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

    public static int Run(string[] args)
    {
        if (args.Length > 0 && args[0].Trim().Length > 0) _namesrv = args[0].Trim();
        string stamp = (NowMs() % 1000000).ToString(CultureInfo.InvariantCulture);
        string dropTopic = "OffsetIllegalDrop_" + stamp;
        string persistTopic = "OffsetIllegalPersist_" + stamp;
        string dropGroup = "GID_OffsetIllegalDrop_" + stamp;
        string persistGroup = "GID_OffsetIllegalPersist_" + stamp;
        Console.WriteLine("namesrv=" + _namesrv + " topic1=" + dropTopic + " topic2=" + persistTopic);

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

        var setup = new MQClientInstance("iloffsetup-" + stamp, new List<string> { _namesrv });
        setup.Start();
        var producer = new DefaultMQProducer("OffsetIllegalLive_pg_" + stamp)
        {
            NamesrvAddr = _namesrv,
            InstanceName = "offset-illegal-live-" + stamp,
        };

        try
        {
        // Explicitly creating the topic with 1 queue: S1's window (3 messages on one queue)
     // and S2's out-of-range (103 > maxOffset) both require a countable queue count and
        // cannot rely on the broker's default 4 queues.
            setup.CreateTopicInRoute(dropTopic, 1, 1);
            setup.CreateTopicInRoute(persistTopic, 1, 1);
            producer.Start();
            Thread.Sleep(1000);

            ScenarioDropAndRebuild(producer, admin, brokerAddr, dropTopic, dropGroup, stamp);
            ScenarioImmediatePersist(producer, admin, brokerAddr, persistTopic, persistGroup, stamp);
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

            foreach (string t in new[] { dropTopic, persistTopic })
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

            foreach (string g in new[] { dropGroup, persistGroup })
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

    private static void ScenarioDropAndRebuild(DefaultMQProducer producer, DefaultMQAdminExt admin,
        string brokerAddr, string topic, string group, string stamp)
    {
        Console.WriteLine("=== S1 OFFSET_ILLEGAL: invalidate the in-flight/buffer messages as a whole and rebuild at the corrected offset ===");
        var listener = new GatedListener();
        var c = new DefaultMQPushConsumer(group)
        {
            InstanceName = "offset-illegal-drop-" + stamp,
            ConsumeMessageBatchMaxSize = 1,
            ConsumeFromWhere = RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        c.SetNamesrvAddr(_namesrv);
        c.SetMessageListener(listener);
        c.Subscribe(topic, "*");
        c.Start();

        try
        {
            List<MessageQueue> queues = new();
            bool oneQueue = WaitUntil(() =>
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
            Check("S1-the topic has exactly 1 queue (3 messages on one queue, the window of 1 in flight + 2 in the buffer only holds then)",
                oneQueue, "queues=" + N(queues.Count));
            if (!oneQueue) return;

            MessageQueue mq = queues[0];
            string key = Key(mq);

            for (int i = 0; i < Msgs; ++i)
            {
                producer.Send(new Message(topic, Str2Bytes("ilo-" + N(i))));
            }

            int pending = -1;
            int inflight = -1;
            bool arranged = WaitUntil(() =>
            {
                pending = c.PendingForTest(key).Count;
                inflight = c.InFlightForTest(key);
                return pending == Msgs - 1 && inflight == 1;
            }, 30000, 200);
          Check("S1-window ready: 1 in flight (the listener is gated) + 2 left in the buffer", arranged,
                "pending=" + N(pending) + " inflight=" + N(inflight)
                + " arrivals=" + Join(listener.Offsets()));
            if (!arranged) return;

     // RPC2 writes resetOffsetTable -> the next pull gets short-circuited by PullMessageProcessor into OFFSET_RESET
            admin.ResetOffsetByQueueId(brokerAddr, group, topic, 0, Msgs);
            Console.WriteLine("S1: resetOffsetByQueueId(->" + N(Msgs)
        + ") has been sent, waiting for the next pull to pick up the server-side reset...");

            bool dropped = WaitUntil(() => c.QueueEpochForTest(key) >= 1, 45000, 200);
   Check("S1-after the broker judges the offset illegal this side drops the queue (ProcessQueue generation +1)", dropped,
                "epoch=" + N(c.QueueEpochForTest(key)) + " arrivals=" + Join(listener.Offsets()));

            bool committed = WaitUntil(() =>
                Probe(admin, group, mq).Committed == Msgs, 20000);
      Check("S1-the broker-side offset stops at the corrected value 3 (this scenario's two reset RPCs already wrote it once, a weak assertion)",
                committed, "committed=" + N(Probe(admin, group, mq).Committed));

            listener.Release();
            Thread.Sleep(6000);
            List<long> arr = listener.Offsets();
       Check("S1-the 2 messages already fetched into the buffer are invalidated as a whole (messages 1 and 2 are never delivered)",
                arr.Count > 0 && !arr.Contains(1) && !arr.Contains(2), "arrivals=" + Join(arr));

            producer.Send(new Message(topic, Str2Bytes("ilo-after")));
            bool resumed = WaitUntil(() => listener.Offsets().Contains(Msgs), 30000);
            arr = listener.Offsets();
    Check("S1-the rebuilt queue resumes pulling from the corrected offset (the new message 3 is delivered, the history already taken is not redelivered)",
                resumed && !arr.Contains(1) && !arr.Contains(2), "arrivals=" + Join(arr));

            bool advanced = WaitUntil(() => Probe(admin, group, mq).Committed == Msgs + 1, 25000);
            Check("S1-the freeze is lifted by the rebuild (the new message's ack lets the broker offset advance to 4)",
                advanced, "committed=" + N(Probe(admin, group, mq).Committed));
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

    private static void ScenarioImmediatePersist(DefaultMQProducer producer, DefaultMQAdminExt admin,
        string brokerAddr, string topic, string group, string stamp)
    {
        Console.WriteLine("=== S2 OFFSET_ILLEGAL: the correction flushes the corrected offset immediately (not on the periodic flush) ===");
        var first = new RecordingListener();
        var c = new DefaultMQPushConsumer(group)
        {
            InstanceName = "offset-illegal-persist-" + stamp,
            ConsumeFromWhere = RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        c.SetNamesrvAddr(_namesrv);
        c.SetMessageListener(first);
        c.Subscribe(topic, "*");
        c.Start();

        List<MessageQueue> queues = new();
        bool oneQueue = WaitUntil(() =>
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
        Check("S2-the topic has exactly 1 queue", oneQueue, "queues=" + N(queues.Count));
        if (!oneQueue)
        {
            c.Shutdown();
            return;
        }

        MessageQueue mq = queues[0];
        for (int i = 0; i < Msgs; ++i)
        {
            producer.Send(new Message(topic, Str2Bytes("ilo2-" + N(i))));
        }

        bool consumed = WaitUntil(() => first.Offsets().Count == Msgs, 30000);
    Check("S2-precondition: the consumer consumed the 3 normally first", consumed, "arrivals=" + Join(first.Offsets()));
        // shutdown persists synchronously once, after which the broker-side offset is 3; it
  // must be stopped first, otherwise its periodic/on-shutdown flush would write back over
        // the illegal value planted below.
        c.Shutdown();
        Thread.Sleep(1000);

        bool rejected = false;
        string remark = string.Empty;
        try
        {
            admin.ResetOffsetByQueueId(brokerAddr, group, topic, 0, IllegalTarget);
        }
        catch (Exception e)
        {
            rejected = true;
            string msg = e.Message;
            remark = e.GetType().Name + ": "
                + (msg.Length > 140 ? msg.Substring(0, 140) : msg);
        }

        Check("S2-precondition: the out-of-range target was rejected by resetOffsetInner (the 2nd RPC)", rejected, remark);

        (long seeded, long seededMax) = Probe(admin, group, mq);
     Check("S2-precondition: the 1st commitOffset already persisted the illegal offset 103 (the two RPCs are not atomic)",
            seeded == IllegalTarget, "committed=" + N(seeded) + " maxOffset=" + N(seededMax));

        var second = new RecordingListener();
        var c2 = new DefaultMQPushConsumer(group)
        {
            InstanceName = "offset-illegal-persist2-" + stamp,
            ConsumeFromWhere = RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset,
        // The periodic flush stretched to 60s: the only path inside the window that can
        // rewrite the broker offset is the immediate persist the correction branch brings
            PersistConsumerOffsetIntervalMillis = 60000,
        };
        c2.SetNamesrvAddr(_namesrv);
        c2.SetMessageListener(second);
        c2.Subscribe(topic, "*");
        long t0 = NowMs();
        c2.Start();
        Console.WriteLine("S2: the new consumer pulls from the illegal offset " + N(IllegalTarget) + ", waiting for the correction to write the broker offset back to "
            + N(Msgs) + " (periodic flush = 60s)...");

        bool back = WaitUntil(() => Probe(admin, group, mq).Committed == Msgs, 20000, 500);
        Check("S2-the broker offset is corrected from 103 back to 3 (the persist the correction branch brings itself)", back,
            "elapsed=" + N((NowMs() - t0) / 1000) + "s committed="
            + N(Probe(admin, group, mq).Committed));

     // Wait through another quiet window: once the offset is corrected back it does not go
        // back to redeliver 0..2 and is not rewritten either
        Thread.Sleep(6000);
        Check("S2-zero deliveries the whole way (the corrected offset lands after the history so nothing is dispatched)",
            second.Offsets().Count == 0, "arrivals=" + Join(second.Offsets()));
        Check("S2-after the quiet window the offset is still 3", Probe(admin, group, mq).Committed == Msgs,
            "committed=" + N(Probe(admin, group, mq).Committed));
        c2.Shutdown();
    }

    private static int Report()
    {
        Console.WriteLine("############ PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
            + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture) + " ############");
        return _fail == 0 ? 0 : 1;
    }
}
