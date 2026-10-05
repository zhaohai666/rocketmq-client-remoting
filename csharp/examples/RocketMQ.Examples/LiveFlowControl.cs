// 拉取前流控（Java ProcessQueue 的五个阈值）真机验证
//（对应 python/verify_flow_control_live.py、cpp/examples/live_flow_control.cpp、
//  rust/examples/live_flow_control.rs 的 S0–S5，判据逐条同构）。
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
//   S5 启动期数值闸门（Java checkConfig :1099-1209）：区间边界值真机能跑起来并收全消息，
//      越界配置在本地就被拒，且 broker 侧查不到这个组（没留下僵尸 clientId 撑歪 cidAll）。
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

    /// <summary>"把这道闸门压到不命中"的合法写法：Java checkConfig(:1099-1209) 给的
    /// **上界**（pullThresholdForQueue / consumeConcurrentlyMaxSpan 都是 [1, 65535]），
    /// <b>不是</b> int.MaxValue、也不是 0。两者都会在 Start() 被拒：0 越下界，
    /// 20 亿越上界 —— 而校验是这次改动新加的，夹具必须跟着改成 Java 也认可的写法。</summary>
    private const int Huge = 65535;

    /// <summary>字节闸门"关闭"的写法同上：pullThresholdSizeForQueue 合法域 [1, 1024] MiB。</summary>
    private const int HugeSizeMiB = 1024;

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
        c.Consumer.PullThresholdSizeForQueue = HugeSizeMiB;
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
        c.Consumer.PullThresholdSizeForQueue = HugeSizeMiB;
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

    // ---------------- S5 启动期数值闸门（Java checkConfig :1099-1209）----------------
    // 离线单测（tests/ConsumerCheckConfigTests.cs）锁的是区间与文案；这里补两件只有
    // 真集群能锁死的事：
    //   1. 落在 Java 区间**边界**上的配置真能把消费者跑起来并收全消息 —— 闸门写歪最常
    //      见的方式是"比 Java 还严"，把合法配置也拒了，用户直接起不来；
    //   2. 越界的配置**没有打到 broker 上**。写成"先注册再校验"的话，broker 的
    //      ConsumerManager 会留下一堆永不心跳的僵尸 clientId，把 rebalance 用的 cidAll
    //      撑歪（真机表现为队列分配不均），而客户端日志里只有启动失败那一条。
    private static void S5CheckConfig(DefaultMQProducer producer)
    {
        string topic = Topic("Config");
        PrepareTopic(producer, topic, 4);
        string goodGroup = Group("5");
        string badGroup = Group("6");

        // 每条闸门都取 Java 区间的端点值：PullBatchSize=1024、PopInvisibleTime=300000
        // 这类"贴着上限"的写法在生产里就是"实际不拦"，误拒等于把用户挡在门外。
        Case c = StartConsumer("5", topic, 0);
        c.Consumer.SetConsumeThreadMin(1);
        c.Consumer.SetConsumeThreadMax(2);
        c.Consumer.ConsumeConcurrentlyMaxSpan = Huge;
        c.Consumer.PullThresholdForQueue = Huge;
        c.Consumer.PullThresholdForTopic = -1;
        c.Consumer.PullThresholdSizeForQueue = HugeSizeMiB;
        c.Consumer.PullThresholdSizeForTopic = -1;
        c.Consumer.PullIntervalMillis = 0;
        c.Consumer.ConsumeMessageBatchMaxSize = 1;
        c.Consumer.PullBatchSize = 1024;
        c.Consumer.PopInvisibleTime = DefaultMQPushConsumer.MaxPopInvisibleTime;
        c.Consumer.PopBatchNums = 32;
        bool started = true;
        try
        {
            c.Consumer.Start();
        }
        catch (Exception e)
        {
            started = false;
            Check("S5-边界值配置能启动", false, e.Message);
        }

        if (started)
        {
            Check("S5-边界值配置能启动", true, "IsStarted=" + c.Consumer.IsStarted);
            Sleep(3000);
            for (int i = 0; i < 10; ++i)
            {
                producer.Send(new Message(topic, Encoding.UTF8.GetBytes("c-" + i)), 5000);
            }

            bool all = WaitUntil(() => c.Sink.Got() >= 10, 30000);
            Check("S5-边界值配置下 10 条全到达",
                all && c.Sink.Got() == 10 && c.Sink.Distinct() == 10,
                "got=" + c.Sink.Got() + " distinct=" + c.Sink.Distinct());
        }

        // 越界配置：本地拒（文案逐字对 Java）+ 失败后不留半启动实例。
        // 每条都取"刚刚越界"的值：差 1 就够，越界幅度大不代表更可信。
        (Action<DefaultMQPushConsumer> Break, string Want)[] bad =
        {
            (x => x.PullThresholdSizeForQueue = 0,
             "pullThresholdSizeForQueue Out of range [1, 1024]"),
            (x => x.PullBatchSize = 1025, "pullBatchSize Out of range [1, 1024]"),
            (x => x.PopInvisibleTime = DefaultMQPushConsumer.MinPopInvisibleTime - 1,
             "popInvisibleTime Out of range [5000, 300000]"),
            (x => x.PopBatchNums = 33, "popBatchNums Out of range [1, 32]"),
            (x =>
            {
                x.SetConsumeThreadMin(8);
                x.SetConsumeThreadMax(4);
            }, "consumeThreadMin (8) is larger than consumeThreadMax (4)"),
        };
        foreach ((Action<DefaultMQPushConsumer> brk, string want) in bad)
        {
            var badConsumer = new DefaultMQPushConsumer(badGroup)
            {
                InstanceName = "live-fc-bad-" + _stamp,
            };
            badConsumer.SetNamesrvAddr(_namesrv);
            badConsumer.Subscribe(topic, "*");
            badConsumer.SetMessageListener(new Sink(0));
            brk(badConsumer);
            bool rejected = false;
            string actual;
            try
            {
                badConsumer.Start();
                actual = "Start() 居然成功了";
                badConsumer.Shutdown();
            }
            catch (MQClientException e)
            {
                rejected = true;
                actual = e.Message;
            }
            catch (Exception e)
            {
                actual = "别的异常: " + e.Message;
            }

            Check("S5-越界配置被拒: " + want, rejected && actual == want, "实际=" + actual);
            Check("S5-越界配置没留下半启动实例: " + want, !badConsumer.IsStarted);
        }

        if (!started)
        {
            return;
        }

        // broker 侧反证：被拒的组查不到、边界值组查得到。
        // 必须用**裸**的 GetConsumerListByGroup —— GetConsumerIdListByGroup 内部吞异常
        // 返回 null，"被拒绝"与"没注册"在调用方看来一模一样（LiveAcl.cs 的 S6 同注）。
        var probe = new MQClientInstance("FC_CS_PROBE_" + _stamp, new List<string> { _namesrv });
        probe.Start();
        try
        {
            string addr = probe.BrokerAddrForTopic(topic);
            Check("S5-拿到 broker 地址用于查消费组", addr.Length > 0, "addr=" + addr);
            if (addr.Length == 0)
            {
                return;
            }

            // 从未注册过的组：broker 不回空列表，而是直接甩 code=1
            // "no consumer for this group"。两种形态都算"查无此组"，但绝不能带 clientId。
            bool absent;
            string detail;
            try
            {
                var ids = probe.GetConsumerListByGroup(badGroup, addr, 5000);
                absent = ids.ConsumerIdList.Count == 0;
                detail = "ids=" + ids.ConsumerIdList.Count;
            }
            catch (MQBrokerException e)
            {
                absent = true;
                detail = "broker 直接拒绝: code=" + e.ResponseCode + " " + e.ResponseMessage;
            }
            catch (Exception e)
            {
                absent = false;
                detail = "探测请求失败: " + e.Message;
            }

            Check("S5-broker 侧不知道被拒的消费组", absent, detail);

            int goodN = 0;
            string goodDetail = string.Empty;
            try
            {
                goodN = probe.GetConsumerListByGroup(goodGroup, addr, 5000).ConsumerIdList.Count;
            }
            catch (Exception e)
            {
                goodDetail = "探测失败: " + e.Message;
            }

            Check("S5-broker 侧认下了边界值消费者", goodN == 1,
                "n=" + goodN + (goodDetail.Length > 0 ? " " + goodDetail : ""));
        }
        finally
        {
            probe.Shutdown();
            c.Consumer.Shutdown();
        }
    }

    private static void Cleanup()
    {
        foreach (string kind in new[] { "Defaults", "Size", "Span", "TopicCount", "Config" })
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
                S5CheckConfig(producer);
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
