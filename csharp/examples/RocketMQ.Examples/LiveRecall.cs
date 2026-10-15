// Real-machine verification of scheduled message recall (recallMessage 370).
// Usage: rmq recall [namesrv]
//
// Why it must be a real machine: the handle cannot be assembled by the client itself. Only
// the broker inside SendMessageProcessor#attachRecallHandle (and only when the message
// carries TIMER_* delay properties) attaches the handle back to the SEND response header;
// and whether the recall really took effect can only be proven by "it was not delivered at
// the due time". The offline unit tests (tests/RecallMessageTests.cs) pin the codec, the
// wire key names and the local validation order, which cannot prove the semantics.
using System.Diagnostics;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveRecall
{
    private const string Broker = "broker-a";
    private const string ConfigKey = "recallMessageEnable";
    private const int DelaySec = 12;

    private static string _namesrv = "127.0.0.1:9876";
    private static string _prefix = string.Empty;
    private static string _topic = string.Empty;
    private static string _group = string.Empty;

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

    private static string Num(int v) => v.ToString(CultureInfo.InvariantCulture);

    private static string Str(byte[] body) => Encoding.UTF8.GetString(body);

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    private sealed class Collector : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<string> _bodies = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk) foreach (MessageExt m in msgs) _bodies.Add(Str(m.Body));
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public List<string> Snapshot()
        {
            lock (_lk) return new List<string>(_bodies);
        }
    }

    public static int Run(string[] args)
    {
        _namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        _prefix = "RecallDotnet_" + Num((int)(DateTimeOffset.UtcNow.ToUnixTimeMilliseconds() % 1000000));
        _topic = _prefix + "_Topic";
        _group = _prefix + "_Group";
        Console.WriteLine("namesrv = " + _namesrv + "  prefix = " + _prefix);

        var producer = new DefaultMQProducer(_group + "_prod")
        {
            NamesrvAddr = _namesrv,
            SendMsgTimeout = 5000,
        };
        producer.Start();

        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_namesrv);
        admin.Start();

        string? original = null;
        try
        {
            string addr = BrokerAddr(producer);

            // ---- R0 the recall switch: readable + temporarily enabled ----
            original = ReadFlag(admin, addr);
            Check("R0 can read the broker's " + ConfigKey, original is not null, "value=" + original);
            if (original != "true")
            {
                WriteFlag(admin, addr, "true");
            }

            if (ReadFlag(admin, addr) != "true")
            {
                Check("R0 recall is not enabled on the broker, so the later checks are meaningless", false);
                return 1;
            }

            // ---- R1/R2 only a scheduled message carries a handle, and its content matches the send result ----
            Message toRecall = Timer("to-recall");
            Message control = Timer("control");
            var plain = new Message(_topic, Bytes("plain"));

            SendResult rRecall = producer.Send(toRecall);
            SendResult rControl = producer.Send(control);
            SendResult rPlain = producer.Send(plain);
            string? handle = rRecall.RecallHandle;
            Check("R1 the scheduled message brings back a recallHandle", !string.IsNullOrEmpty(handle), "handle=" + handle);
            Check("R1 a normal message carries no recallHandle", rPlain.RecallHandle is null);
            Check("R1 the two scheduled messages have different handles",
                !string.IsNullOrEmpty(handle) && rControl.RecallHandle != handle);

            try
            {
                HandleV1 parsed = RecallMessageHandle.DecodeHandle(handle);
                Check("R2 the broker's handle can be decoded by our codec", true);
                Check("R2 the topic/brokerName in the handle match the send target",
                    parsed.Topic == _topic && parsed.BrokerName == Broker,
                    "topic=" + parsed.Topic + " broker=" + parsed.BrokerName);
                Check("R2 the uniqKey in the handle is this message's UNIQ_KEY",
                    parsed.MessageId == rRecall.MsgId,
                    "handle=" + parsed.MessageId + " send=" + rRecall.MsgId);
            }
            catch (Exception e)
            {
                Check("R2 the broker's handle can be decoded by our codec", false, e.Message);
            }

            // ---- R4/R5 the local validation must complete before any network access ----
            Check("R4 a %RETRY% topic is rejected",
                Recall(producer, "%RETRY%" + _group, handle ?? string.Empty) == "topic is not supported");
            Check("R4 a %DLQ% topic is rejected",
                Recall(producer, "%DLQ%" + _group, handle ?? string.Empty) == "topic is not supported");

            var began = Stopwatch.GetTimestamp();
            string corrupt = Recall(producer, _topic, "not-a-handle");
            double costMs = Stopwatch.GetElapsedTime(began).TotalMilliseconds;
            Check("R5 an illegal handle is rejected locally", corrupt == RecallMessageHandle.InvalidHandle,
                corrupt + " (" + costMs.ToString("F3", CultureInfo.InvariantCulture) + "ms)");
            Check("R5 an illegal handle comes back instantly (no network access)", costMs < 200, Num((int)costMs) + "ms");

            // ---- R3/R6 it really was recalled, and the control message was not dragged in ----
            string recalled = Recall(producer, _topic, handle ?? string.Empty);
            Check("R3 RecallMessage returns the uniqKey of the recalled message",
                recalled == rRecall.MsgId, "resp=" + recalled + " send=" + rRecall.MsgId);

            List<string> got = ConsumeFor(DelaySec + 20);
            var bodies = new HashSet<string>(got);
            Check("R6 the control scheduled message is delivered on time", bodies.Contains("control"), "got=" + Sorted(bodies));
            Check("R6 the normal message was delivered", bodies.Contains("plain"));
            Check("R6 the recalled scheduled message was never delivered", !bodies.Contains("to-recall"),
                "got=" + Sorted(bodies));
        }
        finally
        {
            bool restored = false;
            if (original is not null)
            {
                try
                {
                    WriteFlag(admin, BrokerAddr(producer), original);
                    restored = ReadFlag(admin, BrokerAddr(producer)) == original;
                }
                catch (Exception e)
                {
                    Console.WriteLine("  [diag] restore failed: " + e.Message);
                }
            }

            Check("R7 the broker's " + ConfigKey + " has been restored to " + original, restored);
            admin.Shutdown();
            producer.Shutdown();
        }

        Console.WriteLine();
        Console.WriteLine("PASS=" + Num(_pass) + " FAIL=" + Num(_fail));
        return _fail == 0 ? 0 : 1;
    }

    private static Message Timer(string body)
    {
        var m = new Message(_topic, Bytes(body));
        m.PutProperty("TIMER_DELAY_SEC", Num(DelaySec));
        return m;
    }

    private static string Sorted(IEnumerable<string> items) =>
        string.Join(",", items.OrderBy(s => s, StringComparer.Ordinal));

    /// <summary>Recalls and returns the exception text as the return value for the checks
    /// (the normal path returns the uniqKey the broker gave).</summary>
    private static string Recall(DefaultMQProducer producer, string topic, string handle)
    {
        try
        {
            return producer.RecallMessage(topic, handle);
        }
        catch (Exception e)
        {
            return e.Message;
        }
    }

    private static string BrokerAddr(DefaultMQProducer producer)
    {
        string addr = producer.Client().BrokerAddrOf(Broker);
        return addr.Length > 0 ? addr : "127.0.0.1:10911";
    }

    private static string? ReadFlag(DefaultMQAdminExt admin, string addr)
    {
        try
        {
            PropertyMap conf = admin.GetBrokerConfig(addr, 5000);
            return conf.TryGetValue(ConfigKey, out string? v) ? v : null;
        }
        catch (Exception e)
        {
            Console.WriteLine("  [diag] GetBrokerConfig failed: " + e.Message);
            return null;
        }
    }

    private static void WriteFlag(DefaultMQAdminExt admin, string addr, string value)
    {
        var props = new PropertyMap { [ConfigKey] = value };
        try
        {
            admin.UpdateBrokerConfig(addr, props, 5000);
        }
        catch (Exception e)
        {
            Console.WriteLine("  [diag] UpdateBrokerConfig failed: " + e.Message);
        }
    }

    /// <summary>Collects a whole time window starting from offset 0 and returns the list of
    /// bodies received.
    /// The window must be &gt; the delay: the control message only appears once it is due,
    /// while the recalled one has to "not appear for the entire window" for the recall to
    /// count as successful.</summary>
    private static List<string> ConsumeFor(int windowSec)
    {
        var received = new Collector();
        var consumer = new DefaultMQPushConsumer(_group + "_c");
        consumer.SetNamesrvAddr(_namesrv);
        consumer.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
        consumer.SetMessageListener(received);
        consumer.Subscribe(_topic, "*");
        consumer.Start();
        Thread.Sleep(windowSec * 1000);
        consumer.Shutdown();
        return received.Snapshot();
    }
}
