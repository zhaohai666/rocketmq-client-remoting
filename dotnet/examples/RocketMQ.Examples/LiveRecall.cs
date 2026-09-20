// 定时消息撤回（recallMessage 370）真机验证。
// 用法：rmq recall [namesrv]
//
// 为什么必须真机：句柄不是客户端能自己拼出来的。只有 broker 在
// SendMessageProcessor#attachRecallHandle 里（且仅当消息带 TIMER_* 延迟属性）才会把句柄
// 挂回 SEND 响应头；撤回是否真的生效也只能靠「到点没投递」来证明。
// 离线单测（tests/RecallMessageTests.cs）锁的是编解码、报文键名和本地校验顺序，证不了语义。
//
// R0 读得到 broker 的 recallMessageEnable，并临时打开
// R1 定时消息带回句柄、普通消息不带、两条定时消息句柄不同
// R2 句柄能解出 topic / brokerName / uniqKey，且与发送结果一致
// R3 RecallMessage 返回被撤回消息的 uniqKey（Java 取响应头 msgId）
// R4 %RETRY% topic 在本地就被拒（Java "topic is not supported"），不打网络
// R5 非法句柄在本地就被拒（Java "recall handle is invalid"），且是秒回
// R6 **语义**：同时发两条同样延迟的消息，撤回其中一条 → 到点后对照消息被投递、
//    被撤回的那条永远不到；这才是撤回真正的定义
// R7 全程把 broker 的 recallMessageEnable 改回原值（跑之前是 false 就跑完还是 false）
//
// broker 的 recallMessageEnable 默认 false（Java BrokerConfig:546），本地 conf 也是 false，
// 所以先 UPDATE_BROKER_CONFIG 打开，退出前无条件还原。
//
// 与 python/verify_recall_live.py、cpp/examples/recall_live.cpp、
// rust/examples/live_producer.rs 的 P9 段同题。
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

            // ---- R0 撤回开关：读得到 + 临时打开 ----
            original = ReadFlag(admin, addr);
            Check("R0 能读到 broker 的 " + ConfigKey, original is not null, "value=" + original);
            if (original != "true")
            {
                WriteFlag(admin, addr, "true");
            }

            if (ReadFlag(admin, addr) != "true")
            {
                Check("R0 recall 未在 broker 上开启，后续检查无意义", false);
                return 1;
            }

            // ---- R1/R2 定时消息才带句柄，且内容与发送结果一致 ----
            Message toRecall = Timer("to-recall");
            Message control = Timer("control");
            var plain = new Message(_topic, Bytes("plain"));

            SendResult rRecall = producer.Send(toRecall);
            SendResult rControl = producer.Send(control);
            SendResult rPlain = producer.Send(plain);
            string? handle = rRecall.RecallHandle;
            Check("R1 定时消息带回 recallHandle", !string.IsNullOrEmpty(handle), "handle=" + handle);
            Check("R1 普通消息不带 recallHandle", rPlain.RecallHandle is null);
            Check("R1 两条定时消息的句柄不同",
                !string.IsNullOrEmpty(handle) && rControl.RecallHandle != handle);

            try
            {
                HandleV1 parsed = RecallMessageHandle.DecodeHandle(handle);
                Check("R2 broker 的句柄能被我们的编解码器解开", true);
                Check("R2 句柄里的 topic/brokerName 与发送目标一致",
                    parsed.Topic == _topic && parsed.BrokerName == Broker,
                    "topic=" + parsed.Topic + " broker=" + parsed.BrokerName);
                Check("R2 句柄里的 uniqKey 就是这条消息的 UNIQ_KEY",
                    parsed.MessageId == rRecall.MsgId,
                    "handle=" + parsed.MessageId + " send=" + rRecall.MsgId);
            }
            catch (Exception e)
            {
                Check("R2 broker 的句柄能被我们的编解码器解开", false, e.Message);
            }

            // ---- R4/R5 本地校验必须在打网络之前跑完 ----
            Check("R4 %RETRY% topic 被拒",
                Recall(producer, "%RETRY%" + _group, handle ?? string.Empty) == "topic is not supported");
            Check("R4 %DLQ% topic 被拒",
                Recall(producer, "%DLQ%" + _group, handle ?? string.Empty) == "topic is not supported");

            var began = Stopwatch.GetTimestamp();
            string corrupt = Recall(producer, _topic, "not-a-handle");
            double costMs = Stopwatch.GetElapsedTime(began).TotalMilliseconds;
            Check("R5 非法句柄本地即拒", corrupt == RecallMessageHandle.InvalidHandle,
                corrupt + " (" + costMs.ToString("F3", CultureInfo.InvariantCulture) + "ms)");
            Check("R5 非法句柄是秒回的（没打网络）", costMs < 200, Num((int)costMs) + "ms");

            // ---- R3/R6 真的撤掉了，且没牵连对照消息 ----
            string recalled = Recall(producer, _topic, handle ?? string.Empty);
            Check("R3 RecallMessage 返回被撤回消息的 uniqKey",
                recalled == rRecall.MsgId, "resp=" + recalled + " send=" + rRecall.MsgId);

            List<string> got = ConsumeFor(DelaySec + 20);
            var bodies = new HashSet<string>(got);
            Check("R6 对照定时消息按时投递", bodies.Contains("control"), "got=" + Sorted(bodies));
            Check("R6 普通消息已投递", bodies.Contains("plain"));
            Check("R6 被撤回的定时消息永远没投递", !bodies.Contains("to-recall"),
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

            Check("R7 broker 的 " + ConfigKey + " 已还原为 " + original, restored);
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

    /// <summary>撤回并把异常文案当返回值给检查用（正常路径返回 broker 给的 uniqKey）。</summary>
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

    /// <summary>从 0 位点收满一个时间窗，返回收到的 body 列表。
    /// 窗口必须 &gt; 延迟：对照消息要等到点才出现，被撤回的那条则要「整个窗口都不出现」
    /// 才算撤回成功。</summary>
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
