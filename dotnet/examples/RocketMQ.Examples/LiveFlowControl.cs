// 拉取前流控（Java ProcessQueue 的五个阈值）真机验证
//（对应 python/verify_flow_control_live.py、cpp/examples/live_flow_control.cpp、
//  rust/examples/live_flow_control.rs 的 S0–S4，判据逐条同构）。
//
// 离线单测（tests/FlowControlTests.cs）锁的是**判据本身**；这里锁真机上两件离线永远
// 锁不住的事：
//   A 闸门在真实 broker 上**确实会命中**。单位错一位、阈值读错一个字段，离线拿预置
//     缓冲照样"能命中"，真机上却永远不命中（或永远命中）。Rust 就曾在
//     pullThresholdSizeForTopic 那道闸门上误用队列级开关，离线全绿。
//   B 命中之后**一条消息都不许丢**。流控只是"暂停拉取"，不是"丢弃/跳过"：暂停期间
//     位点不许越过还没消费完的消息，恢复后同一个队列必须继续消费到末尾。写成"命中就
//     丢批 / 退出循环"在十几秒窗口里完全看不出来，只有把全部消息数完才暴露。
//
// 场景：
//   S0 默认闸门 + 快消费：不该命中（triggered==0），12 条全到 —— 防闸门误伤正常流量。
//   S1 队列级字节闸门：条数/跨度放到不可能命中，size=1(MiB) + 400KB 不可压缩大消息 + 慢消费。
//   S2 位点跨度闸门：条数/字节都关掉，只剩 consumeConcurrentlyMaxSpan=2。
//   S3 topic 级条数闸门：队列级三条全关掉，只剩 pullThresholdForTopic=4 —— 必须跨队列累计。
//   S4 命中之后恢复：复用 S1 的组与 topic 再来一批大消息，闸门仍命中且新消息照单全收
//      （锁"暂停 100ms"被写成"退出拉取循环"的错误 —— S1 看不出差别）。
//
// ⚠ 大消息必须**不可压缩**：生产者对超过压缩阈值的 body 先试压，全同字节的 payload 会被
//   压到几百字节，broker 落盘的 StoreSize 跟着变几百字节 —— "size 闸门永不命中"就成了
//   夹具问题（Python 侧第一次跑正是这么踩到的）。
// ⚠ S1/S4 的 topic 必须只有 **1 条队列**：8 条 400KB 摊到 4 条队列上每条才 800KB，
//   永远够不到 1MiB 这道**队列级**闸门。
//
// 用法：rmq flow-control [namesrv]（需本地 5.5.1 集群）
using System.Security.Cryptography;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveFlowControl
{
    private static string _namesrv = "127.0.0.1:9876";
    private static string _stamp = string.Empty;
    private static DefaultMQAdminExt _admin = null!;

    private static int _pass;
    private static int _fail;

    /// <summary>远大于任何真机缓冲的阈值，等价于把那道闸门关掉（不写 int.MaxValue 以免加法溢出）。</summary>
    private const int Huge = 2000000000;

    private const int KiB = 1024;

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

    private static void Sleep(int ms) => Thread.Sleep(ms);

    private static bool WaitUntil(Func<bool> pred, int millis)
    {
        DateTime deadline = DateTime.UtcNow.AddMilliseconds(millis);
        for (;;)
        {
            if (pred()) return true;
            if (DateTime.UtcNow >= deadline) return false;
            Sleep(100);
        }
    }

    private static string Topic(string kind) => "FcCs_" + _stamp + "_" + kind;

    private static string Group(string kind) => "fc_cs_" + _stamp + "_g" + kind;

    /// <summary>显式建 topic（不靠 autoCreateTopicEnable，队列数才确定 —— S1 依赖"就是 1 条队列"）。</summary>
    private static void PrepareTopic(DefaultMQProducer producer, string topic, int queues)
    {
        try
        {
            producer.CreateTopic("init", topic, queues);
        }
        catch (Exception e)
        {
            Console.WriteLine("  预建 topic " + topic + " 失败（改用自动创建）: " + e.Message);
        }

        // 等 NameServer 路由传播，否则消费者首轮 rebalance 仍查不到
        Sleep(3000);
    }

    /// <summary>400KB **不可压缩**消息体：前缀 + 随机尾巴（前缀只便于日志辨认，判据一律用条数）。</summary>
    private static byte[] BigBody(int tag)
    {
        byte[] body = new byte[400 * KiB];
        RandomNumberGenerator.Fill(body);
        byte[] prefix = Encoding.UTF8.GetBytes("FCBIG-" + tag.ToString("D3") + "-");
        Array.Copy(prefix, body, prefix.Length);
        return body;
    }

    /// <summary>收消息用：记录 body，可选地每批睡 slowMs（制造"已拉未消费"的堆积）。</summary>
    private sealed class Sink : IMessageListenerConcurrently
    {
        private readonly int _slowMs;
        private readonly object _lk = new();
        private readonly HashSet<string> _bodies = new(StringComparer.Ordinal);
        private int _got;

        public Sink(int slowMs) => _slowMs = slowMs;

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
        {
            if (_slowMs > 0) Sleep(_slowMs);
            lock (_lk)
            {
                foreach (MessageExt m in msgs)
                {
                    // 用长度 + 前缀当键：400KB 的整份 body 进 HashSet 只是白占内存
                    _bodies.Add(m.Body.Length + ":" + Encoding.UTF8.GetString(m.Body, 0,
                        Math.Min(m.Body.Length, 16)));
                    ++_got;
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public int Got()
        {
            lock (_lk) return _got;
        }

        public int Distinct()
        {
            lock (_lk) return _bodies.Count;
        }
    }

    private sealed class Case
    {
        public required DefaultMQPushConsumer Consumer;
        public required Sink Sink;
    }

    private static Case StartConsumer(string kind, string topic, int slowMs)
    {
        var sink = new Sink(slowMs);
        var consumer = new DefaultMQPushConsumer(Group(kind));
        consumer.SetNamesrvAddr(_namesrv);
        consumer.InstanceName = "live-fc-" + kind + "-" + _stamp;
        consumer.Subscribe(topic, "*");
        consumer.SetMessageListener(sink);
        return new Case { Consumer = consumer, Sink = sink };
    }

    private static void SendSmall(DefaultMQProducer producer, string topic, int n)
    {
        for (int i = 0; i < n; ++i)
        {
            producer.Send(new Message(topic, Encoding.UTF8.GetBytes("ok-" + i)), 5000);
        }
    }

    private static void SendBig(DefaultMQProducer producer, string topic, int n)
    {
        for (int i = 0; i < n; ++i)
        {
            producer.Send(new Message(topic, BigBody(i)), 20000);
        }
    }

    // ------------------------------------------------ S0 默认闸门 + 快消费
    private static void S0Defaults(DefaultMQProducer producer)
    {
        string topic = Topic("Defaults");
        PrepareTopic(producer, topic, 4);
        Case c = StartConsumer("0", topic, 0);
        c.Consumer.Start();
        Sleep(3000);
        SendSmall(producer, topic, 12);
        bool all = WaitUntil(() => c.Sink.Got() >= 12, 30000);
        long fc = c.Consumer.FlowControlTriggered;
        c.Consumer.Shutdown();
        Check("S0-默认闸门不命中", fc == 0, "triggered=" + fc);
        Check("S0-默认闸门下全部到达", all && c.Sink.Got() == 12, "got=" + c.Sink.Got());
        Check("S0-一条不重不丢", c.Sink.Distinct() == 12, "distinct=" + c.Sink.Distinct());
    }

    // ------------------------------ S1 队列级字节闸门（topic 必须单队列）
    private static void S1SizeGate(DefaultMQProducer producer)
    {
        string topic = Topic("Size");
        PrepareTopic(producer, topic, 1);
        Case c = StartConsumer("1", topic, 300);
        c.Consumer.PullThresholdForQueue = Huge;
        c.Consumer.PullThresholdSizeForQueue = 1;      // 1 MiB
        c.Consumer.ConsumeConcurrentlyMaxSpan = Huge;
        c.Consumer.Start();
        Sleep(3000);
        SendBig(producer, topic, 8);
        bool hit = WaitUntil(() => c.Consumer.FlowControlTriggered > 0, 30000);
        bool all = WaitUntil(() => c.Sink.Got() >= 8, 40000);
        long fc = c.Consumer.FlowControlTriggered;
        c.Consumer.Shutdown();
        Check("S1-队列级字节闸门真机命中", hit, "triggered=" + fc);
        Check("S1-大消息一条不丢", all && c.Sink.Got() == 8, "got=" + c.Sink.Got());
        Check("S1-8 条 400KB 消息全不重复", c.Sink.Distinct() == 8,
            "distinct=" + c.Sink.Distinct());
    }

    // ------------------------------------------------------ S2 位点跨度闸门
    private static void S2SpanGate(DefaultMQProducer producer)
    {
        string topic = Topic("Span");
        PrepareTopic(producer, topic, 4);
        Case c = StartConsumer("2", topic, 300);
        c.Consumer.PullThresholdForQueue = Huge;
        c.Consumer.PullThresholdSizeForQueue = 0;      // 0 = 这条闸门关闭
        c.Consumer.ConsumeConcurrentlyMaxSpan = 2;
        c.Consumer.Start();
        Sleep(3000);
        SendSmall(producer, topic, 14);
        bool hit = WaitUntil(() => c.Consumer.FlowControlTriggered > 0, 30000);
        bool all = WaitUntil(() => c.Sink.Got() >= 14, 40000);
        long fc = c.Consumer.FlowControlTriggered;
        c.Consumer.Shutdown();
        Check("S2-跨度闸门真机命中", hit, "triggered=" + fc);
        Check("S2-跨度过限后仍全部消费", all && c.Sink.Got() == 14, "got=" + c.Sink.Got());
    }

    // -------------------------------------------------- S3 topic 级条数闸门
    private static void S3TopicGate(DefaultMQProducer producer)
    {
        string topic = Topic("TopicCount");
        PrepareTopic(producer, topic, 4);
        Case c = StartConsumer("3", topic, 300);
        c.Consumer.PullThresholdForQueue = Huge;
        c.Consumer.PullThresholdSizeForQueue = 0;
        c.Consumer.ConsumeConcurrentlyMaxSpan = Huge;
        c.Consumer.PullThresholdForTopic = 4;
        c.Consumer.Start();
        Sleep(3000);
        SendSmall(producer, topic, 16);
        bool hit = WaitUntil(() => c.Consumer.FlowControlTriggered > 0, 30000);
        bool all = WaitUntil(() => c.Sink.Got() >= 16, 40000);
        long fc = c.Consumer.FlowControlTriggered;
        c.Consumer.Shutdown();
        Check("S3-topic 级条数闸门真机命中", hit, "triggered=" + fc);
        Check("S3-跨队列累计后仍全部消费", all && c.Sink.Got() == 16, "got=" + c.Sink.Got());
    }

    // ------------------------------------- S4 命中之后恢复（复用 S1 的组与 topic）
    private static void S4Recovery(DefaultMQProducer producer)
    {
        // 位点已由 S1 提交到 broker 末尾，这里只会收到新的一批。
        string topic = Topic("Size");
        Case c = StartConsumer("1", topic, 300);
        c.Consumer.PullThresholdForQueue = Huge;
        c.Consumer.PullThresholdSizeForQueue = 1;
        c.Consumer.ConsumeConcurrentlyMaxSpan = Huge;
        c.Consumer.Start();
        Sleep(3000);
        SendBig(producer, topic, 6);
        bool all = WaitUntil(() => c.Sink.Got() >= 6, 40000);
        bool hit = WaitUntil(() => c.Consumer.FlowControlTriggered > 0, 30000);
        long fc = c.Consumer.FlowControlTriggered;
        c.Consumer.Shutdown();
        Check("S4-触发过流控的队列恢复后继续消费", all && c.Sink.Got() == 6,
            "got=" + c.Sink.Got());
        Check("S4-恢复批次仍然命中流控（闸门不会命中一次后失效）", hit, "triggered=" + fc);
        Check("S4-恢复批次不重复", c.Sink.Distinct() == 6, "distinct=" + c.Sink.Distinct());
    }

    private static void Cleanup()
    {
        foreach (string kind in new[] { "Defaults", "Size", "Span", "TopicCount" })
        {
            try
            {
                _admin.DeleteTopic(Topic(kind));
            }
            catch (Exception e)
            {
                Console.WriteLine("  [diag] DeleteTopic(" + Topic(kind) + ") failed: " + e.Message);
            }
        }
    }

    public static int Run(string[] args)
    {
        _namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        _stamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds().ToString();
        Console.WriteLine("== 流控真机验证：" + _namesrv + " stamp=" + _stamp + " ==");

        var producer = new DefaultMQProducer("PID_fc_cs_" + _stamp)
        {
            NamesrvAddr = _namesrv,
            InstanceName = "fc-cs-producer-" + _stamp,
            SendMsgTimeout = 20000,
        };
        producer.Start();
        _admin = new DefaultMQAdminExt("FCCSADMIN");
        _admin.SetNamesrvAddr(_namesrv);
        _admin.SetTimeoutMillis(10000);
        _admin.Start();

        try
        {
            // 集群探活：拿不到 broker 注册就别往下跑，否则每条判据都只是在等超时
            bool alive = WaitUntil(() =>
            {
                try
                {
                    return producer.FetchPublishMessageQueues(MixAll.DefaultTopic).Count > 0;
                }
                catch (Exception)
                {
                    return false;
                }
            }, 30000);
            if (!alive)
            {
                Check("集群探活", false, "nameServer 上没有 " + MixAll.DefaultTopic + " 的路由");
            }
            else
            {
                Check("集群探活", true, "broker 队列数=" +
                    producer.FetchPublishMessageQueues(MixAll.DefaultTopic).Count);
                S0Defaults(producer);
                S1SizeGate(producer);
                S2SpanGate(producer);
                S3TopicGate(producer);
                S4Recovery(producer);
            }
        }
        catch (Exception e)
        {
            Check("联调异常", false, e.Message);
        }

        Cleanup();
        producer.Shutdown();
        _admin.Shutdown();
        Console.WriteLine();
        Console.WriteLine("PASS=" + Volatile.Read(ref _pass) + " FAIL=" + Volatile.Read(ref _fail));
        return Volatile.Read(ref _fail) == 0 ? 0 : 1;
    }
}
