// Real-machine integration of the three send header fields (defaultTopic / defaultTopicQueueNums / brokerName) (scenario sections H0-H5).
//
//
// Usage: rmq send-header [namesrv] (needs a local 5.5.1 cluster, autoCreateTopicEnable=true)
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveSendHeader
{
    private static string _namesrv = "127.0.0.1:9876";
    private static string _stamp = string.Empty;
    private static DefaultMQAdminExt _admin = null!;
    private static string _brokerAddr = string.Empty;
    private static string _brokerName = string.Empty;

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

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private static void Sleep(int ms) => Thread.Sleep(ms);

    private static bool WaitUntil(Func<bool> pred, int seconds)
    {
        DateTime deadline = DateTime.UtcNow.AddSeconds(seconds);
        for (;;)
        {
            if (pred()) return true;
            if (DateTime.UtcNow >= deadline) return false;
            Sleep(100);
        }
    }

    /// <summary>A group of topics isolated by stamp; Cleanup sweeps them afterwards.</summary>
    private static readonly List<string> Topics = new();

    private static string Topic(string kind)
    {
        string t = "Hdr" + kind + "_" + _stamp;
        Topics.Add(t);
        return t;
    }

    /// <summary>The terminal state of one async send.</summary>
    private sealed class Latch : ISendCallback
    {
        private readonly object _lk = new();
        private readonly List<SendResult> _oks = new();
        private readonly List<Exception> _errors = new();

        public void OnSuccess(SendResult sendResult)
        {
            lock (_lk) _oks.Add(sendResult);
        }

        public void OnException(Exception error)
        {
            lock (_lk) _errors.Add(error);
        }

        public bool WaitDone(int ms) => WaitUntil(() =>
        {
            lock (_lk) return _oks.Count + _errors.Count > 0;
        }, ms / 1000);

        public bool FirstOk()
        {
            lock (_lk) return _oks.Count == 1 && _errors.Count == 0
                && _oks[0].SendStatus == SendStatus.SendOk;
        }

        public string Errors()
        {
            lock (_lk) return string.Join(";", _errors.Select(e => e.Message));
        }
    }

    /// <summary>
    /// Configures and starts a producer. instanceName is given explicitly so that the
    /// clientId is predictable; passing null for the two knobs c/d means keeping the
    /// defaults (H1 uses the defaults as its baseline).
    /// </summary>
    private sealed class ScopedProducer : IDisposable
    {
        private readonly DefaultMQProducer _producer;

        public ScopedProducer(string kind, string? createTopicKey = null,
            int? defaultTopicQueueNums = null)
        {
            _producer = new DefaultMQProducer("PID_send_header_" + _stamp)
            {
                NamesrvAddr = _namesrv,
                InstanceName = "hdr-" + kind + "-" + _stamp,
                SendMsgTimeout = 5000,
            };
            if (createTopicKey is not null) _producer.CreateTopicKey = createTopicKey;
            if (defaultTopicQueueNums is not null)
            {
                _producer.DefaultTopicQueueNums = defaultTopicQueueNums.Value;
            }
            _producer.Start();
        }

        public DefaultMQProducer Producer => _producer;

        public SendResult Send(string topic, string body) =>
            _producer.Send(new Message(topic, Bytes(body)), 5000);

        public void Dispose()
        {
            try
            {
                _producer.Shutdown();
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] producer shutdown failed: " + e.Message);
            }
        }
    }

    /// <summary>Reads back the (read, write) queue count of that topic on the broker; returns
    /// null when it does not exist yet.</summary>
    private static (int Read, int Write)? QueueNums(string topic)
    {
        try
        {
            TopicConfig cfg = _admin.ExamineTopicConfig(_brokerAddr, topic);
            return (cfg.ReadQueueNums, cfg.WriteQueueNums);
        }
        catch (Exception)
        {
            return null;
        }
    }

    /// <summary>Waits for the topic to appear on the broker (auto topic creation happens
    /// synchronously in the send path and takes a moment to land).</summary>
    private static (int Read, int Write) WaitForTopic(string topic)
    {
        WaitUntil(() => QueueNums(topic) is not null, 10);
        return QueueNums(topic) ?? (-1, -1);
    }

    /// <summary>The number of stored messages of that topic across all brokers (a new
    /// topic's minOffset is always 0); returns -1 when it cannot be read.</summary>
    private static long TotalMessages(string topic)
    {
        try
        {
            TopicStatsTable stats = _admin.ExamineTopicStats(topic);
            long total = 0;
            foreach (KeyValuePair<MessageQueue, TopicOffset> kv in stats.OffsetTable)
            {
                total += kv.Value.MaxOffset - kv.Value.MinOffset;
            }
            return total;
        }
        catch (Exception)
        {
            return -1;
        }
    }

    // ------------------------------------------------- H0/H1/H2/H3 queue counts
    private static void H0ToH3QueueNums()
    {
        // H0: TBW102 is this broker's real "template topic", and the criteria of H2/H3 both depend on its queue count
        (int Read, int Write)? tbw = QueueNums(MixAll.DefaultTopic);
        if (tbw is null || tbw.Value.Write <= 0)
        {
            Check("H0 the TBW102 template is readable", false, "examineTopicConfig(TBW102) cannot be read");
            return;
        }
        Check("H0 the TBW102 template is readable", true, "read=" + tbw.Value.Read + " write=" + tbw.Value.Write);

        // H1: set nothing, d goes with MixAll.DefaultTopicQueueNums(4)
        string t1 = Topic("Def");
        using (ScopedProducer p1 = new("h1"))
        {
            SendResult r1 = p1.Send(t1, "h1");
            Check("H1 the send with the default config succeeded", r1.SendStatus == SendStatus.SendOk,
                "msgId=" + r1.MsgId + " broker=" + r1.MessageQueue.BrokerName);
        }
        (int Read, int Write) n1 = WaitForTopic(t1);
        int want1 = Math.Min(MixAll.DefaultTopicQueueNums, tbw.Value.Write);
        Check("H1 a topic created with the default d=4 has min(4, TBW102) queues",
            n1.Write == want1 && n1.Read == n1.Write,
            "read=" + n1.Read + " write=" + n1.Write + " (TBW102=" + tbw.Value.Write + ")");

        // H2: d=2 must carry the queue count down -- before the fix it was hard-coded to 4
        string t2 = Topic("Nums");
        using (ScopedProducer p2 = new("h2", null, 2))
        {
            p2.Send(t2, "h2");
        }
        (int Read, int Write) n2 = WaitForTopic(t2);
        Check("H2 a topic created with DefaultTopicQueueNums=2 has only 2 queues",
            n2.Write == Math.Min(2, tbw.Value.Write) && n2.Read == n2.Write,
            "read=" + n2.Read + " write=" + n2.Write + " (the old hard-coded-4 behaviour would be " + n1.Write + ")");

        // H3: c points at its own template topic. The template must carry PERM_INHERIT,
        // otherwise isInherited at TopicConfigManager:286 fails and the broker refuses to
        // auto-create the topic
        string src = Topic("Src");
        try
        {
            _admin.CreateTopicInBroker(_brokerAddr, src, 3, 3,
                PermName.PermRead | PermName.PermWrite | PermName.PermInherit);
        }
        catch (Exception e)
        {
            Check("H3 the template topic was created", false, e.Message);
            return;
        }
        (int Read, int Write) nsrc = WaitForTopic(src);
        Check("H3 the template topic is built (3 queues, with INHERIT)",
            nsrc.Write == 3 && !(nsrc.Read == tbw.Value.Read && nsrc.Write == tbw.Value.Write),
            "src=(" + nsrc.Read + "," + nsrc.Write + ") tbw102=(" + tbw.Value.Read + "," +
            tbw.Value.Write + ")");

        string t3 = Topic("Inherit");
        using (ScopedProducer p3 = new("h3", src, 8))
        {
            p3.Send(t3, "h3");
        }
        (int Read, int Write) n3 = WaitForTopic(t3);
        Check("H3 createTopicKey=the template topic is inherited (min(8,3)=3 rather than TBW102's " +
            tbw.Value.Write + ")", n3.Write == 3 && n3.Read == 3,
            "read=" + n3.Read + " write=" + n3.Write);
    }

    // ------------------------------------------------------------- H4 five entry points
    private static void H4SendEntries()
    {
        string t4 = Topic("Entries");
        Dictionary<string, string> landed = new();
        Latch latch = new();
        using (ScopedProducer p = new("h4"))
        {
            SendResult rs = p.Send(t4, "h4-sync");
            landed["sync"] = rs.SendStatus == SendStatus.SendOk ? "" : "status=" + rs.SendStatus;

            // pinned send: mq given explicitly, landing on a different call chain (the broker name comes from the caller)
            MessageQueue mq = rs.MessageQueue;
            SendResult rb = p.Producer.Send(new Message(t4, Bytes("h4-pinned")), mq, 5000);
            landed["pinned"] = rb.SendStatus == SendStatus.SendOk
                && rb.MessageQueue.BrokerName == mq.BrokerName
                ? "" : "status=" + rb.SendStatus + " broker=" + rb.MessageQueue.BrokerName;

            try
            {
                p.Producer.SendOneway(new Message(t4, Bytes("h4-oneway")));
                landed["oneway"] = "";  // oneway has no response, so only the later total can back it up
            }
            catch (Exception e)
            {
                landed["oneway"] = e.Message;
            }

            List<Message> batch = new();
            for (int i = 0; i < 3; i++) batch.Add(new Message(t4, Bytes("h4-batch-" + i)));
            SendResult rr = p.Producer.SendBatch(batch, 5000);
            landed["batch"] = rr.SendStatus == SendStatus.SendOk ? "" : "status=" + rr.SendStatus;

            p.Producer.SendAsync(new Message(t4, Bytes("h4-async")), latch, 5000);
            landed["async"] = latch.WaitDone(10000) && latch.FirstOk()
                ? "" : "the callback did not get a unique SEND_OK: " + latch.Errors();
        }

        foreach (string entry in new[] { "sync", "pinned", "oneway", "batch", "async" })
        {
            string why = landed.TryGetValue(entry, out string? v) ? v : "that entry point recorded nothing";
            Check("H4 the " + entry + " entry point sent successfully", why.Length == 0, why);
        }
        // 1 synchronous + 1 pinned + 1 oneway + 3 batch + 1 async = 7 messages
        bool got = WaitUntil(() => TotalMessages(t4) >= 7, 20);
        Check("H4 all seven messages landed one by one (a batch counts per sub-message)", got && TotalMessages(t4) == 7,
            "total=" + TotalMessages(t4));
    }

    // ------------------------------------------------------------- H5 brokerName
    private static void H5BrokerNameMatchesRoute()
    {
        string t5 = Topic("BrokerName");
        SendResult r5;
        using (ScopedProducer p = new("h5"))
        {
            r5 = p.Send(t5, "h5");
        }
        Check("H5 the landing broker name matches the route (n is it)",
            r5.SendStatus == SendStatus.SendOk && r5.MessageQueue.BrokerName == _brokerName,
            "landed on=" + r5.MessageQueue.BrokerName + " route=" + _brokerName);
    }

    private static void Cleanup()
    {
        foreach (string t in Topics)
        {
            try
            {
                _admin.DeleteTopic(t);
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] DeleteTopic(" + t + ") failed: " + e.Message);
            }
        }
    }

    public static int Run(string[] args)
    {
        _namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        _stamp = DateTimeOffset.UtcNow.ToUnixTimeSeconds().ToString();
        Console.WriteLine("namesrv=" + _namesrv + " stamp=" + _stamp);

        _admin = new DefaultMQAdminExt("HDRDOTNETADMIN");
        _admin.SetNamesrvAddr(_namesrv);
        _admin.SetTimeoutMillis(10000);
        _admin.Start();

        try
        {
            // cluster liveness: take the broker address and look its brokerName back up (the criterion of H5)
            ClusterInfo? cluster = null;
            WaitUntil(() =>
            {
                try
                {
                    cluster = _admin.FetchBrokerClusterInfo();
                    return cluster.BrokerAddrTable.Count > 0;
                }
                catch (Exception)
                {
                    return false;
                }
            }, 30);
            if (cluster is null || cluster.BrokerAddrTable.Count == 0)
            {
                Check("the cluster liveness probe", false, "the nameServer has no broker registered");
            }
            else
            {
                foreach (KeyValuePair<string, BrokerData> kv in cluster.BrokerAddrTable)
                {
                    _brokerAddr = kv.Value.SelectBrokerAddr();
                    _brokerName = kv.Key;
                    break;
                }
                Check("the cluster liveness probe", _brokerAddr.Length > 0,
                    "broker=" + _brokerAddr + " name=" + _brokerName);

                H0ToH3QueueNums();
                H4SendEntries();
                H5BrokerNameMatchesRoute();
            }
        }
        catch (Exception e)
        {
            Check("the integration threw", false, e.Message);
        }

        Cleanup();
        _admin.Shutdown();
        Console.WriteLine();
        Console.WriteLine("PASS=" + Volatile.Read(ref _pass) + " FAIL=" + Volatile.Read(ref _fail));
        return Volatile.Read(ref _fail) == 0 ? 0 : 1;
    }
}
