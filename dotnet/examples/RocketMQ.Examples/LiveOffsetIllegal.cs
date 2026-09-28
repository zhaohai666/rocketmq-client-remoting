// OFFSET_ILLEGAL 纠错分支（Java DefaultMQPushConsumerImpl:402-427）真机联调
//（对应 python/verify_offset_illegal_live.py、cpp/examples/live_offset_illegal.cpp、
//  rust/examples/live_offset_illegal.rs 的 S1/S2）。
//
// 离线单测（tests/RocketMQ.Client.Tests/OffsetIllegalRecoverTests.cs，13 项）只能锁住本地状态
// 怎么清、哪个 ack 被作废、冻结什么时候解；下面两件事只有真集群能证明：
//
//   S1 「丢队列」—— broker 判定位点非法时，这条队列上**已取回但还没消费/没 ack** 的消息必须
//       整批作废。做法：listener 卡住第一条第 0 条（在途 1 条、缓冲里 2 条），再用
//       resetOffsetByQueueId 把服务端位点重置到 3（下一笔 pull 被 PullMessageProcessor 短路成
//       OFFSET_RESET ⇒ 客户端 PullStatus.OffsetIllegal，修正值在应答头 nextBeginOffset）。
//       修复前：缓冲里的第 1、2 条照常投递；修复后：只剩在途的第 0 条，且它的 ack 因队列
//       已被丢而作废。最后再发第 4 条，验证重建后的队列从修正位点续跑、冻结已随重建解除
//       （新消息的 ack 让 broker 上的位点继续前进到 4）。
//
//   S2 「立刻落盘」—— 纠错后的位点必须马上推给 broker，不能等周期落盘。做法：利用
//       resetOffsetByQueueId 两笔 RPC 非原子（第 1 笔 commitOffset 无区间校验先落库、第 2 笔
//       被 resetOffsetInner 以 "Target offset N not in consume queue range" 拒绝）的既有行为，
//       把 broker 上的已提交位点做成非法值 103，再让一个 persistConsumerOffsetInterval=60000
//       的新消费者从 103 起拉。窗口内唯一能把 103 写回 3（maxOffset）的路径就是纠错分支自带
//       的那次 persist，且全程零投递。
//
// ⚠ 发现延迟是 Java 同构的长轮询语义：客户端下发 suspendTimeoutMillis=20000（Java
//   PullAPIWrapper.brokerSuspendMaxTimeMillis 默认值），broker 的 PullRequestHoldService 每 5s
//   巡检一次到期请求，命中前那笔 pull 不会重读 resetOffsetTable（实测 ~24s）。不是缺陷，
//   所以 S1 的等待窗口给到 45s。
//
// 用法：rmq offset-illegal [namesrv]（需本地 5.5.1 集群，autoCreateTopicEnable=true）
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveOffsetIllegal
{
    /// <summary>单队列 topic 上的消息条数：1 条在途 + 2 条缓冲，S1 窗口才成立。</summary>
    private const int Msgs = 3;

    /// <summary>S2 的非法位点：远超该队列 maxOffset(=3)，但又落在 broker 的 int 范围内。</summary>
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

    /// <summary>队列 key（与消费者内部 OffsetKey 一致）：topic + brokerName + queueId。</summary>
    private static string Key(MessageQueue mq) =>
        mq.Topic + mq.BrokerName + mq.QueueId.ToString(CultureInfo.InvariantCulture);

    /// <summary>回读 broker 上的 (已提交位点, maxOffset)；committed = -1 表示查无记录
    /// （QUERY_NOT_FOUND），0 是合法位点，两者不能混同。</summary>
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

    /// <summary>第 1 批卡在闸门上（维持「1 条在途 + 其余在缓冲」的窗口），放行后照常返回成功。
    /// 上限只防死锁：正常路径由 <see cref="Release"/> 显式放行，且必须晚于纠错 —— 在途批次的
    /// ack 是否作废，正是 S1 要证的。</summary>
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

        var setup = new MQClientInstance("iloffsetup-" + stamp, new List<string> { _namesrv });
        setup.Start();
        var producer = new DefaultMQProducer("OffsetIllegalLive_pg_" + stamp)
        {
            NamesrvAddr = _namesrv,
            InstanceName = "offset-illegal-live-" + stamp,
        };

        try
        {
            // 显式 1 队列建 topic：S1 的窗口（3 条同在一条队列上）与 S2 的越界（103 > maxOffset）
            // 都要求可数的队列数，不能靠 broker 的默认 4 队列。
            setup.CreateTopicInRoute(dropTopic, 1, 1);
            setup.CreateTopicInRoute(persistTopic, 1, 1);
            producer.Start();
            Thread.Sleep(1000);

            ScenarioDropAndRebuild(producer, admin, brokerAddr, dropTopic, dropGroup, stamp);
            ScenarioImmediatePersist(producer, admin, brokerAddr, persistTopic, persistGroup, stamp);
        }
        catch (Exception e)
        {
            Check("联调过程中出现未预期异常", false, e.ToString());
        }
        finally
        {
            try
            {
                producer.Shutdown();
            }
            catch (Exception)
            {
                // 收尾失败不掩盖主断言
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

    private static void ScenarioDropAndRebuild(DefaultMQProducer producer, DefaultMQAdminExt admin,
        string brokerAddr, string topic, string group, string stamp)
    {
        Console.WriteLine("=== S1 OFFSET_ILLEGAL：整批作废在途/缓冲消息并按修正位点重建 ===");
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
            Check("S1-topic 恰好 1 个队列（3 条消息同队列，1 在途 + 2 缓冲的窗口才成立）",
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
            Check("S1-窗口就绪：1 条在途（listener 被闸住）+ 2 条留在缓冲", arranged,
                "pending=" + N(pending) + " inflight=" + N(inflight)
                + " arrivals=" + Join(listener.Offsets()));
            if (!arranged) return;

            // RPC2 写 resetOffsetTable → 下一笔 pull 被 PullMessageProcessor 短路成 OFFSET_RESET
            admin.ResetOffsetByQueueId(brokerAddr, group, topic, 0, Msgs);
            Console.WriteLine("S1: 已发出 resetOffsetByQueueId(->" + N(Msgs)
                + ")，等下一笔 pull 取走服务端重置...");

            bool dropped = WaitUntil(() => c.QueueEpochForTest(key) >= 1, 45000, 200);
            Check("S1-broker 判定位点非法后本端丢弃该队列（ProcessQueue 代号 +1）", dropped,
                "epoch=" + N(c.QueueEpochForTest(key)) + " arrivals=" + Join(listener.Offsets()));

            bool committed = WaitUntil(() =>
                Probe(admin, group, mq).Committed == Msgs, 20000);
            Check("S1-broker 上的位点停在修正值 3（本场景两笔重置 RPC 已先写过一次，弱断言）",
                committed, "committed=" + N(Probe(admin, group, mq).Committed));

            listener.Release();
            Thread.Sleep(6000);
            List<long> arr = listener.Offsets();
            Check("S1-缓冲里已取回的 2 条被整批作废（第 1、2 条永不投递）",
                arr.Count > 0 && !arr.Contains(1) && !arr.Contains(2), "arrivals=" + Join(arr));

            producer.Send(new Message(topic, Str2Bytes("ilo-after")));
            bool resumed = WaitUntil(() => listener.Offsets().Contains(Msgs), 30000);
            arr = listener.Offsets();
            Check("S1-重建后的队列从修正位点续拉（第 3 条新消息正常投递，历史拿过的不重投）",
                resumed && !arr.Contains(1) && !arr.Contains(2), "arrivals=" + Join(arr));

            bool advanced = WaitUntil(() => Probe(admin, group, mq).Committed == Msgs + 1, 25000);
            Check("S1-冻结随重建解除（新消息的 ack 让 broker 位点继续前进到 4）",
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
                // 收尾失败不掩盖主断言
            }
        }
    }

    private static void ScenarioImmediatePersist(DefaultMQProducer producer, DefaultMQAdminExt admin,
        string brokerAddr, string topic, string group, string stamp)
    {
        Console.WriteLine("=== S2 OFFSET_ILLEGAL：纠错把修正位点立刻落盘（不等周期落盘）===");
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
        Check("S2-topic 恰好 1 个队列", oneQueue, "queues=" + N(queues.Count));
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
        Check("S2-前置：消费者先正常消费掉 3 条", consumed, "arrivals=" + Join(first.Offsets()));
        // shutdown 会同步 persist 一次，此后 broker 上的位点是 3；必须先停掉它，
        // 否则它的周期/关停落盘会把下面种进去的非法值覆盖回去。
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

        Check("S2-前置：越界目标被 resetOffsetInner 拒绝（第 2 笔 RPC）", rejected, remark);

        (long seeded, long seededMax) = Probe(admin, group, mq);
        Check("S2-前置：第 1 笔 commitOffset 已把非法位点 103 落库（两笔 RPC 非原子）",
            seeded == IllegalTarget, "committed=" + N(seeded) + " maxOffset=" + N(seededMax));

        var second = new RecordingListener();
        var c2 = new DefaultMQPushConsumer(group)
        {
            InstanceName = "offset-illegal-persist2-" + stamp,
            ConsumeFromWhere = RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset,
            // 周期落盘拉长到 60s：窗口内唯一能改写 broker 位点的路径是纠错分支自带的立即 persist
            PersistConsumerOffsetIntervalMillis = 60000,
        };
        c2.SetNamesrvAddr(_namesrv);
        c2.SetMessageListener(second);
        c2.Subscribe(topic, "*");
        long t0 = NowMs();
        c2.Start();
        Console.WriteLine("S2: 新消费者从非法位点 " + N(IllegalTarget) + " 起拉，等纠错把 broker 位点写回 "
            + N(Msgs) + "（周期落盘=60s）...");

        bool back = WaitUntil(() => Probe(admin, group, mq).Committed == Msgs, 20000, 500);
        Check("S2-broker 位点由 103 纠回 3（纠错分支自带的那次 persist）", back,
            "elapsed=" + N((NowMs() - t0) / 1000) + "s committed="
            + N(Probe(admin, group, mq).Committed));

        // 再等一个静默窗口：位点被纠回后不会回头重投 0..2，也不会再被改写
        Thread.Sleep(6000);
        Check("S2-全程零投递（修正位点落在历史消息之后，一条都不下发）",
            second.Offsets().Count == 0, "arrivals=" + Join(second.Offsets()));
        Check("S2-静默窗口后位点仍停在 3", Probe(admin, group, mq).Committed == Msgs,
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
