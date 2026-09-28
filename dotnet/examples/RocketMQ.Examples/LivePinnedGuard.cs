// 定点发送 topic 一致性守卫真机联调（Java DefaultMQProducerImpl:1234-1236 / :1277-1278）
//（对应 python/verify_pinned_guard_live.py、rust/examples/live_pinned_guard.rs、
//  cpp/examples/live_pinned_guard.cpp 的 S0–S6）。
//
// 离线假集群证明的是「拒了、且报文没上线」；真机这一趟要证明的是**另一面**：
//   * 守卫不能误伤真业务 —— 从真实路由取出的队列（broker 名与队列号都是集群给的）在同步
//     单条/同步批量/异步单条/异步批量四条入口上照常 SEND_OK，消息一条不少地被消费到；
//   * 拒的时候要**守在本端** —— 亚毫秒、无 broker 码，而且 **broker 上的 maxOffset 一动不
//     动**：这是真机版的 wire 反证（拒绝不会留下任何痕迹，也不会事后偷发）；
//   * 命名空间腿看 Java queueWithNamespace 的幂等：ns%topic 与裸 topic 都不误拒，
//     ns2%topic 才拒；
//   * 单向定点**没有**守卫（Java :1303-1310 有意留的口子，本端口新补的
//     SendOneway(msg, mq) 同）：报文按 msg 自己的 topic 落库 —— 在真机上用「A 收到、
//     B 的 maxOffset 还是 0」把这条语义钉死。
//
// 场景：
//   S1 正腿：真实路由队列上的同步单条/批量定点发送 SEND_OK，且落在指定队列上
//   S2 反腿：同步单条/批量 topic 不符 ⇒ 本端亚毫秒拒（Java 原文案、无 broker 码），
//          broker 的 maxOffset 不动
//   S3 命名空间：ns%topic 与裸 topic 都放行（且真落库）；ns2%topic 拒（对照腿）
//   S4 异步：单条/批量都在回调里拿到**异步那处文案**；放行腿 SEND_OK
//   S5 单向：无守卫，msg 自己的 topic 说了算（A 收到、B 的 maxOffset 保持 0）
//   S6 收尾：push 消费者把前面所有正腿消息一条不少地收齐
//
// 用法：rmq pinned-guard [namesrv]（需本地 5.5.1 集群，autoCreateTopicEnable=true）
using System.Diagnostics;
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LivePinnedGuard
{
    // 定点发送守卫的两处 Java 文案（同步 :1235、异步 :1278），逐字保留。
    private const string SyncWording = "message's topic not equal mq's topic";
    private const string AsyncWording =
        "Topic of the message does not match its target message queue";

    /// <summary>反腿的耗时上界：守卫是纯字符串比较，真机给 50ms 已留两个数量级余量。</summary>
    private const double LocalBudgetMs = 50.0;

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

    private static string Join(IEnumerable<string> v) => "[" + string.Join(", ", v) + "]";

    private static byte[] Bytes(string s) => Encoding.UTF8.GetBytes(s);

    /// <summary>带 keys 的消息：.NET 只有 (topic, tags, keys, body) 这一个带 keys 的构造。</summary>
    private static Message Keyed(string topic, string body, string keys) =>
        new(topic, null!, keys, Bytes(body));

    private static long NowMs() =>
        DateTimeOffset.UtcNow.ToUnixTimeMilliseconds();

    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 100)
    {
        long deadline = NowMs() + timeoutMs;
        while (NowMs() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(intervalMs);
        }

        return pred();
    }

    /// <summary>等 maxOffset 涨到 want 再返回（broker 的 ConsumeQueue 派发比 SEND 回包慢几毫秒）。</summary>
    private static long WaitOffset(DefaultMQProducer prod, MessageQueue mq, long want,
        int timeoutMs = 5000)
    {
        WaitUntil(() => prod.MaxOffset(mq) >= want, timeoutMs, 50);
        return prod.MaxOffset(mq);
    }

    private static MessageQueue QueueZero(DefaultMQProducer prod, string topic)
    {
        foreach (MessageQueue q in prod.FetchPublishMessageQueues(topic))
        {
            if (q.QueueId == 0) return q;
        }

        throw new InvalidOperationException("topic " + topic + " has no queue 0");
    }

    /// <summary>跑一次定点同步发送，返回 (异常或 null, 耗时毫秒)。</summary>
    private static (MQClientException? Err, double Ms) TimedSend(Func<SendResult> send)
    {
        var sw = Stopwatch.StartNew();
        try
        {
            send();
            return (null, sw.Elapsed.TotalMilliseconds);
        }
        catch (MQClientException e)
        {
            return (e, sw.Elapsed.TotalMilliseconds);
        }
    }

    /// <summary>一次异步发送的终态（成功/异常各记一笔，回调恰好一次）。</summary>
    private sealed class EventSink : ISendCallback
    {
        private readonly object _lk = new();
        private readonly List<(string Kind, SendResult? Result, Exception? Error)> _events = new();

        public void OnSuccess(SendResult sendResult)
        {
            lock (_lk) _events.Add(("ok", sendResult, null));
        }

        public void OnException(Exception error)
        {
            lock (_lk) _events.Add(("err", null, error));
        }

        public bool Done()
        {
            lock (_lk) return _events.Count > 0;
        }

        public (string Kind, SendResult? Result, Exception? Error) First()
        {
            lock (_lk) return _events.Count > 0 ? _events[0] : ("", null, null);
        }
    }

    /// <summary>把正腿消息的 keys 收进一个袋子；CONSUME_FROM_FIRST_OFFSET 免得和发送抢 rebalance。</summary>
    private sealed class KeyListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<string> _keys = new();

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                foreach (MessageExt m in msgs)
                {
                    _keys.Add(m.Keys);
                }
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public List<string> Snapshot()
        {
            lock (_lk) return new List<string>(_keys);
        }
    }

    public static int Run(string[] args)
    {
        if (args.Length > 0 && args[0].Trim().Length > 0) _namesrv = args[0].Trim();
        string stamp = NowMs().ToString(CultureInfo.InvariantCulture);
        string topic = "PinGuard_" + stamp;
        string other = "PinGuardOther_" + stamp;
        string group = "G_pin_guard_" + stamp;
        const string ns = "ns1";
        string wTopic = ns + "%" + topic;
        Console.WriteLine("namesrv=" + _namesrv + " topic=" + topic + " other=" + other);

        var admin = new DefaultMQAdminExt();
        admin.SetNamesrvAddr(_namesrv);
        admin.Start();
        string brokerAddr;
        try
        {
            ClusterInfo cluster = admin.FetchBrokerClusterInfo();
            List<string> addrs = cluster.GetBrokerAddrs();
            if (addrs.Count == 0)
            {
                Check("集群探活", false, "nameServer 无 broker 注册");
                admin.Shutdown();
                return Report();
            }

            brokerAddr = addrs[0];
        }
        catch (Exception e)
        {
            Check("集群探活", false, "fetchBrokerClusterInfo: " + e.Message);
            admin.Shutdown();
            return Report();
        }

        Check("集群探活", true, "broker=" + brokerAddr);

        DefaultMQProducer? prod = null;
        DefaultMQProducer? nsProd = null;
        DefaultMQPushConsumer? push = null;
        var sink = new KeyListener();
        try
        {
            prod = new DefaultMQProducer("PG_pin_guard_" + stamp)
            {
                InstanceName = "pin-guard-" + stamp,
            };
            prod.NamesrvAddr = _namesrv;
            prod.Start();
            prod.CreateTopic(MixAll.DefaultTopic, topic, 4);
            prod.CreateTopic(MixAll.DefaultTopic, other, 4);
            if (!(WaitRoute(prod, topic) && WaitRoute(prod, other)))
            {
                Check("S0 两条 topic 的路由都注册好了", false);
                return Report();
            }

            MessageQueue mq0 = QueueZero(prod, topic);
            // 反腿的目标队列：**真**队列（同 broker 上的另一条 topic queue 0）。
            // 不存在的 broker/队列会先死在路由上，就分不清拒的是 topic 还是地址了。
            MessageQueue mqOther = QueueZero(prod, other);
            Check("S0 两条 topic 的路由都可用（反腿用真队列，拒的才一定是 topic 而不是地址）",
                true, "A=" + mq0.BrokerName + "/" + mq0.QueueId
                + " B=" + mqOther.BrokerName + "/" + mqOther.QueueId);

            // 消费者先起：CONSUME_FROM_FIRST_OFFSET，免得和发送抢 rebalance 的时间点
            push = new DefaultMQPushConsumer(group)
            {
                InstanceName = "pin-guard-live-" + stamp,
                ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
            };
            push.SetNamesrvAddr(_namesrv);
            push.Subscribe(topic, "*");
            push.SetMessageListener(sink);
            push.Start();

            // ---------------- S1 正腿：真路由队列上的定点发送 ----------------
            Console.WriteLine("=== S1 真实路由队列上的定点发送 ===");
            string kSingle = "pinned-live-single";
            SendResult r1 = prod.Send(Keyed(topic, "s1-single", kSingle), mq0, 3000);
            Check("S1a 同步单条定点 SEND_OK 且落在队列 " + mq0.QueueId,
                r1.SendStatus == SendStatus.SendOk && r1.MessageQueue.QueueId == mq0.QueueId
                && r1.MessageQueue.Topic == topic,
                "status=" + SendStatusNames.Name(r1.SendStatus) + " mq=" + r1.MessageQueue);

            string kB1 = "pinned-live-b1";
            string kB2 = "pinned-live-b2";
            SendResult rb = prod.SendBatch(
                new List<Message> { Keyed(topic, "s1-b1", kB1), Keyed(topic, "s1-b2", kB2) },
                mq0, 3000);
            Check("S1b 同步批量定点 SEND_OK 且同一队列",
                rb.SendStatus == SendStatus.SendOk && rb.MessageQueue.QueueId == mq0.QueueId,
                "status=" + SendStatusNames.Name(rb.SendStatus) + " mq=" + rb.MessageQueue);

            long offAfterS1 = WaitOffset(prod, mq0, 3);
            // 批量在消费队列上按**子消息**逐条落位（broker 收到 inner-batch 后拆开写）⇒ 2 条子消息涨 2
            Check("S1c 三笔子消息都真落库（maxOffset = 单条 1 + 批量子消息 2）",
                offAfterS1 == 3, "maxOffset=" + offAfterS1.ToString(CultureInfo.InvariantCulture));

            // ---------------- S2 反腿：同步拒绝，本端、无痕 ----------------
            Console.WriteLine("=== S2 topic 不符：本端亚毫秒拒，broker 上无痕 ===");
            (MQClientException? e, double elapsed) = TimedSend(() =>
                prod.Send(Keyed(topic, "refused", "pinned-live-refused"), mqOther, 3000));
            Check("S2a 同步单条拒（Java 原文案）",
                e is not null && e.Message == SyncWording,
                Fmt(elapsed) + "ms " + (e is null ? "没有抛异常" : e.Message));
            // 本端拒绝的码是 MQClientException 默认 1（UNKNOWN），不是 broker 回码
            Check("S2b 拒在本端：亚毫秒且响应码是客户端默认值（不是超时/不是 broker remark）",
                e is not null && elapsed < LocalBudgetMs && e.ResponseCode == 1,
                Fmt(elapsed) + "ms code=" + (e?.ResponseCode.ToString(CultureInfo.InvariantCulture) ?? "n/a"));
            (MQClientException? e2, double elapsed2) = TimedSend(() =>
                prod.SendBatch(new List<Message> { Keyed(topic, "r1", ""), Keyed(topic, "r2", "") },
                    mqOther, 3000));
            Check("S2c 同步批量共用同一处守卫与同一句文案",
                e2 is not null && e2.Message == SyncWording && elapsed2 < LocalBudgetMs,
                Fmt(elapsed2) + "ms " + (e2 is null ? "没有抛异常" : e2.Message));
            long offA = prod.MaxOffset(mq0);
            long offB = prod.MaxOffset(mqOther);
            Check("S2d wire 反证：A 的 maxOffset 一动没动，B 上一条都没有",
                offA == offAfterS1 && offB == 0, "A=" + offA + " B=" + offB);

            // ---------------- S3 命名空间：wrap 幂等，只拒真的不同名 ----------------
            Console.WriteLine("=== S3 命名空间下的比较（ns%topic 与裸 topic 都不误拒）===");
            nsProd = new DefaultMQProducer("PG_pin_guard_ns_" + stamp)
            {
                InstanceName = "pin-guard-ns-" + stamp,
            };
            nsProd.NamesrvAddr = _namesrv;
            nsProd.Namespace = ns;
            nsProd.Start();
            nsProd.CreateTopic(MixAll.DefaultTopic, wTopic, 4);
            Check("S3a 带前缀 topic 的路由可用", WaitRoute(nsProd, wTopic), "topic=" + wTopic);
            MessageQueue mqw = QueueZero(nsProd, wTopic);
            string kNs1 = "pinned-live-ns-q";
            string kNs2 = "pinned-live-ns-m";
            SendResult n1 = nsProd.Send(Keyed(topic, "ns-wrapped-queue", kNs1), mqw, 3000);
            Check("S3b 队列 topic 已带 ns 前缀：wrap 幂等，不误拒",
                n1.SendStatus == SendStatus.SendOk, "status=" + SendStatusNames.Name(n1.SendStatus));
            SendResult n2 = nsProd.Send(Keyed(wTopic, "ns-wrapped-message", kNs2), mqw, 3000);
            Check("S3c 消息 topic 自己已带前缀同样放行",
                n2.SendStatus == SendStatus.SendOk, "status=" + SendStatusNames.Name(n2.SendStatus));
            (MQClientException? e3, double elapsed3) = TimedSend(() =>
                nsProd.Send(Keyed(topic, "ns2-refused", ""),
                    new MessageQueue("ns2%" + topic, mqw.BrokerName, 0), 3000));
            Check("S3d 换成 ns2% 前缀才拒（对照腿：拒的是名字，不是「有前缀」）",
                e3 is not null && e3.Message == SyncWording && elapsed3 < LocalBudgetMs,
                Fmt(elapsed3) + "ms " + (e3 is null ? "没有抛异常" : e3.Message));
            Check("S3e 两条放行腿真落进 ns1%topic（maxOffset=2）",
                WaitOffset(nsProd, mqw, 2) == 2,
                "maxOffset=" + nsProd.MaxOffset(mqw).ToString(CultureInfo.InvariantCulture));

            // ---------------- S4 异步：回调里是异步那处文案 ----------------
            Console.WriteLine("=== S4 异步单条/批量：拒绝走回调，放行走内核 ===");
            var refused1 = new EventSink();
            prod.SendAsync(Keyed(topic, "async-refused", "pinned-live-async-refused"),
                refused1, 3000, mqOther);
            bool okEv = WaitUntil(refused1.Done, 10000, 50);
            (string kind1, _, Exception? err1) = refused1.First();
            Check("S4a 单条异步拒绝走回调、文案是异步那处",
                okEv && kind1 == "err" && err1 is not null && err1.Message.Contains(AsyncWording),
                kind1 + " " + (err1?.Message ?? ""));
            Check("S4b 拒后 maxOffset 仍不动（异步也没偷发）",
                prod.MaxOffset(mq0) == offAfterS1,
                "maxOffset=" + prod.MaxOffset(mq0).ToString(CultureInfo.InvariantCulture));

            var refused2 = new EventSink();
            prod.SendBatchAsync(new List<Message> { Keyed(topic, "abr1", ""), Keyed(topic, "abr2", "") },
                refused2, 3000, mqOther);
            WaitUntil(refused2.Done, 10000, 50);
            (string kind2, _, Exception? err2) = refused2.First();
            Check("S4c 批量异步共用同一处文案与同一条拒绝路径",
                kind2 == "err" && err2 is not null && err2.Message.Contains(AsyncWording),
                kind2 + " " + (err2?.Message ?? ""));

            string kAs = "pinned-live-async-single";
            string kAb1 = "pinned-live-async-b1";
            string kAb2 = "pinned-live-async-b2";
            var ok1 = new EventSink();
            var ok2 = new EventSink();
            prod.SendAsync(Keyed(topic, "async-ok", kAs), ok1, 3000, mq0);
            prod.SendBatchAsync(new List<Message> { Keyed(topic, "ab1", kAb1), Keyed(topic, "ab2", kAb2) },
                ok2, 3000, mq0);
            WaitUntil(() => ok1.Done() && ok2.Done(), 10000, 50);
            (string kindS, SendResult? resS, _) = ok1.First();
            (string kindB, SendResult? resB, _) = ok2.First();
            Check("S4d 两条放行腿都 SEND_OK",
                kindS == "ok" && resS?.SendStatus == SendStatus.SendOk
                && kindB == "ok" && resB?.SendStatus == SendStatus.SendOk,
                "single=" + kindS + " batch=" + kindB);
            long offAsync = WaitOffset(prod, mq0, offAfterS1 + 3);
            Check("S4e 异步放行腿同样真落库（单条 1 + 批量子消息 2，maxOffset 再涨 3）",
                offAsync == offAfterS1 + 3,
                "maxOffset=" + offAsync.ToString(CultureInfo.InvariantCulture));

            // ---------------- S5 单向：Java 有意没有守卫 ----------------
            Console.WriteLine("=== S5 单向定点没有守卫：msg 自己的 topic 说了算 ===");
            string kOw = "pinned-live-oneway";
            prod.SendOneway(Keyed(topic, "oneway", kOw), mqOther);
            bool landed = WaitUntil(() => prod.MaxOffset(mq0) == offAsync + 1, 10000, 100);
            Check("S5a 单向定点没有守卫：报文按 msg 自己的 topic 落进 A",
                landed, "A maxOffset=" + prod.MaxOffset(mq0).ToString(CultureInfo.InvariantCulture));
            Check("S5b 目标队列所在的 B 一条都没有（是 Java 的口子，不是漏发）",
                prod.MaxOffset(mqOther) == 0,
                "B maxOffset=" + prod.MaxOffset(mqOther).ToString(CultureInfo.InvariantCulture));

            // ---------------- S6 正腿收尾：消息一条不少 ----------------
            Console.WriteLine("=== S6 push 消费者收齐正腿消息 ===");
            var expected = new List<string> { kSingle, kB1, kB2, kAs, kAb1, kAb2, kOw };
            WaitUntil(() =>
            {
                List<string> got = sink.Snapshot();
                return expected.All(k => got.Contains(k));
            }, 40000, 500);
            List<string> received = sink.Snapshot();
            List<string> missing = expected.Where(k => !received.Contains(k)).ToList();
            Check("S6 正腿消息一条不少地被消费到", missing.Count == 0,
                "received=" + Join(received) + " missing=" + Join(missing));
        }
        catch (Exception e)
        {
            Check("联调过程中出现未预期异常", false, e.ToString());
        }
        finally
        {
            foreach (Action shutdown in new Action[]
                     {
                         () => push?.Shutdown(),
                         () => nsProd?.Shutdown(),
                         () => prod?.Shutdown(),
                     })
            {
                try
                {
                    shutdown();
                }
                catch (Exception)
                {
                    // 收尾失败不掩盖主断言
                }
            }

            foreach (string t in new[] { topic, other, wTopic })
            {
                try
                {
                    admin.DeleteTopicInBroker(brokerAddr, t);
                }
                catch (Exception e)
                {
                    Console.WriteLine("  [WARN] deleteTopic(" + t + ") failed: " + e.Message);
                }
            }

            admin.Shutdown();
        }

        return Report();
    }

    private static string Fmt(double ms) => ms.ToString("F2", CultureInfo.InvariantCulture);

    private static bool WaitRoute(DefaultMQProducer prod, string topic, int timeoutMs = 20000)
    {
        return WaitUntil(() =>
        {
            try
            {
                return prod.FetchPublishMessageQueues(topic).Count >= 4;
            }
            catch (MQClientException)
            {
                return false;
            }
        }, timeoutMs, 500);
    }

    private static int Report()
    {
        Console.WriteLine("############ PASS=" + _pass.ToString(CultureInfo.InvariantCulture)
            + " FAIL=" + _fail.ToString(CultureInfo.InvariantCulture) + " ############");
        return _fail == 0 ? 0 : 1;
    }
}
