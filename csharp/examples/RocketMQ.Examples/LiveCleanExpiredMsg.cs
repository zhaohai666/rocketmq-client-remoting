// cleanExpiredMsg 挂起逃生口真机验证（Java ConsumeMessageConcurrentlyService:68-88/192-200
// ＋ ProcessQueue.cleanExpiredMsg:75-127）。
//
// 与 python/verify_clean_expired_msg_live.py、cpp/examples/live_clean_expired_msg.cpp、
// rust/examples/live_clean_expired_msg.rs 的对应场景同题、逐条对应（A0–A5）。
//
// 离线单测（tests/RocketMQ.Client.Tests/CleanExpiredMsgTests.cs）锁的是**判据**
//（选条/阈值/上限/摘除闸门/跳过回投）；这里锁真机上两件离线锁不住的事：
//   A. **清扫真的会开火**：需要一个 listener 挂着不返回超过 ConsumeTimeout 分钟，清扫线程
//      把这条消息 sendMessageBack（delayLevel 3）——整条链路上客户端不能丢它、也不能投两次。
//   B. **重投真的回到 broker**：%RETRY%<group> 的拉取循环独立于挂住的消费线程，消息必须
//      重新出现在本地缓冲、被第二次投递且 ReconsumeTimes=1（broker 侧计数）。
//
// 这条路径坏掉的样子是**静默**的：卡住的消息把该队列位点与分发循环一起钉死，没有任何异常
// 或超时可见，只能从「消息发了却永远不来第二次」反推。
//
// 场景（ConsumeTimeout=1，清扫周期与阈值都是 1 分钟；Java 的过期判据是**严格大于**，
// 所以清扫在第二个 tick 命中，约 start+120s）：
//   A0 业务队列已分配（排除自动订阅的 %RETRY%<group>）。
//   A1 首投在 30s 内到达并挂住；ReconsumeTimes=0；期间在册视图里能看到这条消息
//      且带 CONSUME_START_TIME（本轮盖章）。
//   A2 清扫命中（**核心判据**）：listener 仍挂着时轮询在册视图 —— 消息从里面消失即清扫
//      回投并摘除；距今必须 >60s（排除别的路径动手）。
//   A3 重投真的到了 broker 侧：%RETRY% 队列的本地缓冲里出现这条消息（≤40s）。
//   A4 放行后重新消费：ReconsumeTimes=1、同一条 body 全程只到两次、与首投相隔 >60s。
//   A5 位点收尾：挂住的 listener 返回后业务队列已提交位点走到 1。
//
// 用法：rmq clean-expired-msg [namesrv]   （约 4 分钟，等两个清扫周期）
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

    /// <summary>第一次投递就挂住不返回；放行后恢复正常返回 CONSUME_SUCCESS。</summary>
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
                // 挂起窗口：等清扫动手 + 脚本放行（300s 上限兜底，防止脚本崩了卡死线程）
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

        /// <summary>某 body 的到达记录 [(ReconsumeTimes, 时刻)]。</summary>
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
                Check("集群探活", false, "nameServer 上无 broker 注册");
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
            // 显式 1 队列：A5 的位点口径（== 1）与 A3 的「同一条消息回到 %RETRY%」都要求可数
            setup.CreateTopicInRoute(topic, 1, 1);
            producer.Start();
            Thread.Sleep(3000);
            Console.WriteLine("topic=" + topic + " queues=1");

            consumer = new DefaultMQPushConsumer(group)
            {
                InstanceName = "dotnet-live-ce-" + stamp,
                ConsumeMessageBatchMaxSize = 3,
                ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
                ConsumeTimeout = 1,  // 清扫周期与阈值都变成 1 分钟（Java 同字段同语义）
            };
            consumer.SetNamesrvAddr(_namesrv);
            consumer.SetMessageListener(listener);
            consumer.Subscribe(topic, "*");
            consumer.Start();

            // ---------- A0 业务队列（排除 %RETRY%） ----------
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
            Check("A0-业务队列已分配（%RETRY% 队列不算）", assigned, "key=" + bizKey);
            if (!assigned) return Report();

            // ---------- A1 基线：第一投挂住 ----------
            string body = "ce-dotnet-" + stamp;
            producer.Send(new Message(topic, Str2Bytes(body)));
            bool firstOk = listener.WaitFirst(30000);
            Check("A1-首投在 30s 内到达并挂住", firstOk, "calls=" + N(listener.Calls()));
            List<(int Times, long At)> first = listener.Times(body);
            Check("A1-首投 ReconsumeTimes=0", first.Count > 0 && first[0].Times == 0,
                "times=" + N(first.Count > 0 ? first[0].Times : -1));

            List<MessageExt> entries = consumer.ProcessQueueEntriesForTest(bizKey);
            Check("A1-挂住期间消息登记在册（在 listener 手里）", ContainsBody(entries, body),
                "entries=" + N(entries.Count));
            string stampSeen = entries.Where(m => BodyOf(m) == body)
                .Select(m => m.GetProperty(MessageConst.PropertyConsumeStartTimestamp))
                .FirstOrDefault() ?? string.Empty;
            Check("A1-在册副本带本轮 CONSUME_START_TIME（清扫靠它判过期）",
                stampSeen.Length > 0, "stamp=" + stampSeen);

            // ---------- A2 清扫命中：listener 还挂着，在册视图里已经没了 ----------
            long t0 = NowMs();
            bool swept = WaitUntil(
                () => !ContainsBody(consumer.ProcessQueueEntriesForTest(bizKey), body),
                210000, 1000);
            double elapsed = (NowMs() - t0) / 1000.0;
            Check("A2-清扫在 listener 仍挂起时收走了这条消息（约 start+120s）", swept,
                "elapsed=" + elapsed.ToString("F3", CultureInfo.InvariantCulture) + "s calls="
                + N(listener.Calls()));
            Check("A2-收走时间晚于一个清扫阈值（>60s，排除别的路径动手）", elapsed > 60.0,
                "elapsed=" + elapsed.ToString("F3", CultureInfo.InvariantCulture) + "s");

            // ---------- A3 回投真的到了 broker：%RETRY% 缓冲里出现 ----------
            // 挂起的 listener 把分发线程占住，重投消息只能停在 %RETRY% 队列的本地缓冲里。
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
            Check("A3-回投消息出现在 " + retryTopic + " 的本地缓冲（broker 真收到了 sendMessageBack）",
                back, "retryKey=" + retryKeyFound);

            // ---------- A4 放行：重投被重新消费 ----------
            listener.Release();
            bool second = WaitUntil(() => listener.Times(body).Count >= 2, 60000, 1000);
            List<(int Times, long At)> times = listener.Times(body);
            Check("A4-放行后重新消费到（ReconsumeTimes=1）", second, "times=" + N(times.Count));
            Check("A4-第二次投递 ReconsumeTimes=1（broker 侧重投计数）",
                times.Count >= 2 && times[1].Times == 1,
                "times=" + N(times.Count >= 2 ? times[1].Times : -1));
            Check("A4-同一条 body 全程只到两次（清算一次回投，无重复投递）", times.Count == 2,
                "times=" + N(times.Count));
            long gapSecs = times.Count >= 2 ? (times[1].At - times[0].At) / 1000 : -1;
            Check("A4-第二次投递与首投相隔 >60s（不是 listener 自己造成的重投）", gapSecs > 60,
                "gap=" + N(gapSecs) + "s");

            // ---------- A5 位点收尾 ----------
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
            Check("A5-挂住的 listener 返回后业务队列位点走到 1（Java removeMessage 含已清扫条目）",
                offsetOk, "offset=" + N(committed));
        }
        catch (Exception e)
        {
            Check("联调过程中出现未预期异常", false, e.ToString());
        }
        finally
        {
            // ---------------- 清理 ----------------
            listener.Release();
            if (consumer is not null)
            {
                try
                {
                    consumer.Shutdown();
                }
                catch (Exception e)
                {
                    Console.WriteLine("    (consumer shutdown 失败: " + e.Message + ")");
                }
            }

            try
            {
                admin.DeleteTopic(topic);
                Console.WriteLine("    (deleteTopic(" + topic + ") OK)");
            }
            catch (Exception e)
            {
                Console.WriteLine("    (deleteTopic(" + topic + ") 失败: " + e.Message + ")");
            }

            try
            {
                admin.DeleteSubscriptionGroup(brokerAddr, group, true);
            }
            catch (Exception e)
            {
                Console.WriteLine("    (deleteSubscriptionGroup(" + group + ") 失败: " + e.Message + ")");
            }

            try
            {
                producer.Shutdown();
            }
            catch (Exception)
            {
                // 收尾失败不掩盖主断言
            }

            try
            {
                setup.Shutdown();
            }
            catch (Exception)
            {
                // 收尾失败不掩盖主断言
            }

            try
            {
                admin.Shutdown();
            }
            catch (Exception)
            {
                // 收尾失败不掩盖主断言
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
