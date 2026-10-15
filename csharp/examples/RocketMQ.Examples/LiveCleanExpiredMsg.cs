// Real-machine verification of expired message cleanup (scenario sections A0-A5).
//
// The offline unit tests (tests/RocketMQ.Client.Tests/CleanExpiredMsgTests.cs) pin the
// **criteria** (entry selection / threshold / cap / the removal gate / skipping the resend);
// what is pinned here are the two things offline cannot pin:
//   A. **the sweep really fires**: it takes a listener that hangs for more than
//      ConsumeTimeout minutes, and the sweep thread sendMessageBack's that message
//      (delayLevel 3) -- along the whole chain the client must neither lose it nor send it
//      twice.
//   B. **the resend really gets back to the broker**: the pull loop of %RETRY%<group> is
//      independent of the hung consume thread, so the message must reappear in the local
//      buffer, be delivered a second time and have ReconsumeTimes=1 (the broker's count).
//
// The way this path breaks is **silent**: the hung message pins the queue's offset and the
// dispatch loop together, with no exception or timeout visible, and it can only be deduced
// backwards from "the message was sent but never arrives a second time".
//
//
// Usage: rmq clean-expired-msg [namesrv]   (about 4 minutes, waiting for two sweep periods)
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveCleanExpiredMsg
{
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

    private static byte[] Str2Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static long NowMs() => DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 500)
    {
        long deadline = NowMs() + timeoutMs;
        while (NowMs() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(intervalMs);
        }

        return pred();
    }

    private static string BodyOf(MessageExt m) => Encoding.UTF8.GetString(m.Body);

    private static bool ContainsBody(IEnumerable<MessageExt> msgs, string body)
        => msgs.Any(m => BodyOf(m) == body);

    /// <summary>Returns CONSUME_SUCCESS normally once released; hangs on the first
    /// delivery without returning.</summary>
    private sealed class HungListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<(string Body, int Times, long At)> _arrivals = new();
        private readonly ManualResetEventSlim _firstEntered = new(false);
        private readonly ManualResetEventSlim _released = new(false);
        private int _calls;

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            bool first;
            lock (_lk)
            {
                _calls++;
                first = _calls == 1;
                foreach (MessageExt m in msgs) _arrivals.Add((BodyOf(m), m.ReconsumeTimes, NowMs()));
            }

            if (first)
            {
                _firstEntered.Set();
                // the hang window: wait for the sweep to act + the script to release (a 300s cap as a backstop so a script crash cannot wedge the thread)
                _released.Wait(TimeSpan.FromSeconds(300));
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public bool WaitFirst(int timeoutMs) => _firstEntered.Wait(timeoutMs);

        public void Release() => _released.Set();

        public int Calls()
        {
            lock (_lk) return _calls;
        }

        /// <summary>The arrival records [(ReconsumeTimes, instant)] of a given body.</summary>
        public List<(int Times, long At)> Times(string body)
        {
            lock (_lk)
            {
                return _arrivals.Where(a => a.Body == body)
                    .Select(a => (a.Times, a.At)).ToList();
            }
        }
    }

    public static int Run(string[] args)
    {
        if (args.Length > 0 && args[0].Trim().Length > 0) _namesrv = args[0].Trim();
        string stamp = (NowMs() % 1000000).ToString(CultureInfo.InvariantCulture);
        string topic = "DotnetLiveCe" + stamp;
        string group = "GID_dotnet_live_ce_" + stamp;
        string retryTopic = MixAll.GetRetryTopic(group);
        Console.WriteLine("namesrv=" + _namesrv + " topic=" + topic + " group=" + group);

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

        var setup = new MQClientInstance("celive-setup-" + stamp, new List<string> { _namesrv });
        setup.Start();
        var producer = new DefaultMQProducer("DotnetLiveCe_pg_" + stamp)
        {
            NamesrvAddr = _namesrv,
            InstanceName = "dotnet-live-ce-pg-" + stamp,
        };
        var listener = new HungListener();
        DefaultMQPushConsumer? consumer = null;

        try
        {
            // explicitly 1 queue: A5's offset rule (== 1) and A3's "the very same message comes back to %RETRY%" both require it to be countable
            setup.CreateTopicInRoute(topic, 1, 1);
            producer.Start();
            Thread.Sleep(3000);
            Console.WriteLine("topic=" + topic + " queues=1");

            consumer = new DefaultMQPushConsumer(group)
            {
                InstanceName = "dotnet-live-ce-" + stamp,
                ConsumeMessageBatchMaxSize = 3,
                ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
                ConsumeTimeout = 1,
            };
            consumer.SetNamesrvAddr(_namesrv);
            consumer.SetMessageListener(listener);
            consumer.Subscribe(topic, "*");
            consumer.Start();

            // ---------- A0 the business queue (excluding %RETRY%) ----------
            string bizKey = string.Empty;
            bool assigned = WaitUntil(() =>
            {
                foreach (string k in consumer.AssignedQueueKeys())
                {
                    if (!k.StartsWith(MixAll.RetryGroupTopicPrefix, StringComparison.Ordinal))
                    {
                        bizKey = k;
                        return true;
                    }
                }

                return false;
            }, 30000, 200);
            Check("A0-the business queue is assigned (a %RETRY% queue does not count)", assigned, "key=" + bizKey);
            if (!assigned) return Report();

            // ---------- A1 baseline: the first delivery hangs ----------
            string body = "ce-dotnet-" + stamp;
            producer.Send(new Message(topic, Str2Bytes(body)));
            bool firstOk = listener.WaitFirst(30000);
            Check("A1-the first delivery arrives within 30s and hangs", firstOk, "calls=" + N(listener.Calls()));
            List<(int Times, long At)> first = listener.Times(body);
            Check("A1-the first delivery has ReconsumeTimes=0", first.Count > 0 && first[0].Times == 0,
                "times=" + N(first.Count > 0 ? first[0].Times : -1));

            List<MessageExt> entries = consumer.ProcessQueueEntriesForTest(bizKey);
            Check("A1-while hanging, the message is registered on the books (in the listener's hand)", ContainsBody(entries, body),
                "entries=" + N(entries.Count));
            string stampSeen = entries.Where(m => BodyOf(m) == body)
                .Select(m => m.GetProperty(MessageConst.PropertyConsumeStartTimestamp))
                .FirstOrDefault() ?? string.Empty;
            Check("A1-the registered copy carries this round's CONSUME_START_TIME (the sweep judges expiry by it)",
                stampSeen.Length > 0, "stamp=" + stampSeen);

            // ---------- A2 the sweep hits: the listener is still hung, it is already gone from the view ----------
            long t0 = NowMs();
            bool swept = WaitUntil(
                () => !ContainsBody(consumer.ProcessQueueEntriesForTest(bizKey), body),
                210000, 1000);
            double elapsed = (NowMs() - t0) / 1000.0;
            Check("A2-the sweep took the message away while the listener was still hung (about start+120s)", swept,
                "elapsed=" + elapsed.ToString("F3", CultureInfo.InvariantCulture) + "s calls="
                + N(listener.Calls()));
            Check("A2-the time it was taken away is later than one sweep threshold (>60s, ruling out another path acting)", elapsed > 60.0,
                "elapsed=" + elapsed.ToString("F3", CultureInfo.InvariantCulture) + "s");

            // ---------- A3 the resend really reached the broker: it shows up in %RETRY% ----------
            // The hung listener occupies the dispatch thread, so the resent message can only
            // sit in the local buffer of the %RETRY% queue.
            string retryKeyFound = string.Empty;
            bool back = WaitUntil(() =>
            {
                foreach (string k in consumer.AssignedQueueKeys())
                {
                    if (!k.StartsWith(MixAll.RetryGroupTopicPrefix, StringComparison.Ordinal)) continue;
                    retryKeyFound = k;
                    if (ContainsBody(consumer.PendingForTest(k), body)) return true;
                }

                return false;
            }, 40000, 1000);
            Check("A3-the resent message appears in the local buffer of " + retryTopic + " (the broker really received the sendMessageBack)",
                back, "retryKey=" + retryKeyFound);

            // ---------- A4 release: the resend is consumed again ----------
            listener.Release();
            bool second = WaitUntil(() => listener.Times(body).Count >= 2, 60000, 1000);
            List<(int Times, long At)> times = listener.Times(body);
            Check("A4-after the release it is consumed again (ReconsumeTimes=1)", second, "times=" + N(times.Count));
            Check("A4-the second delivery has ReconsumeTimes=1 (the broker's resend count)",
                times.Count >= 2 && times[1].Times == 1,
                "times=" + N(times.Count >= 2 ? times[1].Times : -1));
            Check("A4-the same body arrives exactly twice in total (one resend settled, no duplicate delivery)", times.Count == 2,
                "times=" + N(times.Count));
            long gapSecs = times.Count >= 2 ? (times[1].At - times[0].At) / 1000 : -1;
            Check("A4-the second delivery is >60s after the first (not a resend caused by the listener itself)", gapSecs > 60,
                "gap=" + N(gapSecs) + "s");

            // ---------- A5 the offset winds up ----------
            List<MessageQueue> queues = consumer.FetchSubscribeMessageQueues(topic);
            MessageQueue mq = queues[0];
            long committed = -2;
            bool offsetOk = WaitUntil(() =>
            {
                try
                {
                    if (admin.ExamineConsumerOffset(group, mq, out long o)) committed = o;
                }
                catch (Exception)
                {
                    committed = -2;
                }

                return committed == 1;
            }, 30000, 1000);
            Check("A5-after the hung listener returns, the business queue's offset reaches 1 (removeMessage includes the swept entry)",
                offsetOk, "offset=" + N(committed));
        }
        catch (Exception e)
        {
            Check("an unexpected exception occurred during the integration", false, e.ToString());
        }
        finally
        {
            // ---------------- cleanup ----------------
            listener.Release();
            if (consumer is not null)
            {
                try
                {
                    consumer.Shutdown();
                }
                catch (Exception e)
                {
                    Console.WriteLine("    (consumer shutdown failed: " + e.Message + ")");
                }
            }

            try
            {
                admin.DeleteTopic(topic);
                Console.WriteLine("    (deleteTopic(" + topic + ") OK)");
            }
            catch (Exception e)
            {
                Console.WriteLine("    (deleteTopic(" + topic + ") failed: " + e.Message + ")");
            }

            try
            {
                admin.DeleteSubscriptionGroup(brokerAddr, group, true);
            }
            catch (Exception e)
            {
                Console.WriteLine("    (deleteSubscriptionGroup(" + group + ") failed: " + e.Message + ")");
            }

            try
            {
                producer.Shutdown();
            }
            catch (Exception)
            {
                // a cleanup failure does not mask the main assertion
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

    private static int Report()
    {
        Console.WriteLine("\nCleanExpiredMsg(dotnet): PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
            + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture));
        return _fail == 0 ? 0 : 1;
    }
}
