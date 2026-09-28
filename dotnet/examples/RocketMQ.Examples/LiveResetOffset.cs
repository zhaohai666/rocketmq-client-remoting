// 220 RESET_CONSUMER_CLIENT_OFFSET（Java MQClientInstance.resetOffset:1403-1450）真机联调
//（对应 python/verify_reset_offset_live.py、cpp/examples/live_reset_offset.cpp、
//  rust/examples/live_reset_offset.rs 的同题场景）。
//
// 220 是 broker 推给**消费端**的重置指令；管理端那笔 INVOKE_BROKER_TO_RESET_OFFSET(222) 的
// 响应只是一张「每个队列重置到哪」的表，真正让消费端改位点的是 broker 随后 oneway 推的
// 220（Broker2Client.resetOffset:181-238）。broker 只在 useServerSideResetOffset=false 时才
// 走这条推送路径（默认 true 在 AdminBrokerProcessor:2255-2263 直接服务端改位点、一台消费端
// 都不通知），所以本验证先把该开关热改成 false（回读确认），跑完还原。
//
// 离线单测（tests/RocketMQ.Client.Tests/ResetOffsetTests.cs）锁死了请求体两种形状与「撤队列 +
// 代号 +1 + 新位点经撤销尾巴落盘」的本地状态；下面这些事只有真集群能证明：
//
//   S1 「回退重置立刻生效 + 在途批次作废」——位点从 10 往回重置到 3，三段判据：
//      a) broker 上的已提交位点在 ~2s 内变成 3：窗口内**只有**重置路径那次 persist 会写它
//         （周期落盘已拉长到 60s），只写内存表的实现在这里原地不动（broker 停在 10）；
//      b) listener 里卡着的旧批次（重置前取回的 offset 10）放行后，它的 ack 必须整批作废
//         —— 采样点：放行旧批次、新队列的**第一批**已进 listener 且还没 ack 时，本地已消费
//         位点必须还是"没有记录 / ≤3"；没有代号闸门的实现这时会跳到 11；
//      c) 队列被真正重建：3..14 每一批都**重投一次**（旧缓冲里没 ack 的 11..14 也随之作废，
//         只能作为重投的一部分出现），放行一轮断言一轮。
//
//   S2 「前跳 + 恢复」——timestamp=-1 重置到 maxOffset：位点直接跳到 10，中间 4..9 一条都不投；
//      随后新消息照常消费、位点继续前进。
//
// 用法：rmq reset-offset [namesrv]（需本地 5.5.1 集群，autoCreateTopicEnable=true）
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LiveResetOffset
{
    /// <summary>单队列 topic 上的消息条数：S1 要 10 条（回退到 3 后重投 3..14），S2 要 10 条。</summary>
    private const int Msgs = 10;

    /// <summary>S1 的回退目标：第 3 条与第 4 条之间按时间戳重置 ⇒ 命中 offset 3。</summary>
    private const long BackTarget = 3;

    private const string SwitchKey = "useServerSideResetOffset";

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

    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 200)
    {
        long deadline = NowMs() + timeoutMs;
        while (NowMs() < deadline)
        {
            if (pred()) return true;
            Thread.Sleep(intervalMs);
        }

        return pred();
    }

    /// <summary>用于"很快就要发生"的断言：采样比 <see cref="WaitUntil"/> 密。</summary>
    private static bool WaitBefore(Func<bool> pred, int windowMs) => WaitUntil(pred, windowMs, 50);

    /// <summary>队列 key（与消费者内部 OffsetKey 一致）：topic + brokerName + queueId。</summary>
    private static string Key(MessageQueue mq) =>
        mq.Topic + mq.BrokerName + mq.QueueId.ToString(CultureInfo.InvariantCulture);

    /// <summary>回读 broker 上的 (已提交位点, maxOffset)；Committed = -1 表示查无记录
    /// （QUERY_NOT_FOUND），0 是合法位点，两者不能混同。</summary>
    private static (long Committed, long Max) Probe(DefaultMQAdminExt admin, string group,
        MessageQueue mq)
    {
        long max;
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

    /// <summary>逐批设闸：每一批都停在闸门上，由测试逐批放行 —— 批与批之间的本地状态因此可以被
    /// 采样。
    ///
    /// S1 的关键采样点（旧批次已放行、新队列第一批还没 ack）只有在"逐批可控"时才存在；一次性
    /// 放行的 listener 会把 11（旧批次 ack）与随后的重投混在一个瞬间里。
    /// 上限只防死锁：正常路径由 <see cref="Release"/> 逐批放行。</summary>
    private sealed class SteppingListener : IMessageListenerConcurrently
    {
        private readonly object _lk = new();
        private readonly List<List<long>> _batches = new();
        private readonly ManualResetEventSlim _release = new(false);

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext ctx)
        {
            lock (_lk)
            {
                var batch = new List<long>();
                foreach (MessageExt m in msgs) batch.Add(m.QueueOffset);
                _batches.Add(batch);
            }

            _release.Wait(TimeSpan.FromSeconds(90));
            _release.Reset(); // 一批一放：与 Python Event.set()+clear() 同构
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }

        public void Release() => _release.Set();

        public int BatchCount()
        {
            lock (_lk) return _batches.Count;
        }

        public List<long> Offsets()
        {
            lock (_lk)
            {
                var all = new List<long>();
                foreach (List<long> b in _batches) all.AddRange(b);
                return all;
            }
        }
    }

    private static bool WaitBatches(SteppingListener l, int n, int timeoutMs) =>
        WaitUntil(() => l.BatchCount() >= n, timeoutMs, 50);

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

    private static bool AllBack(DefaultMQAdminExt admin, string group, MessageQueue mq,
        long want) => Probe(admin, group, mq).Committed == want;

    private static string ReadFlag(DefaultMQAdminExt admin, string brokerAddr, string key)
    {
        try
        {
            PropertyMap cfg = admin.GetBrokerConfig(brokerAddr);
            return cfg.TryGetValue(key, out var v) && v != null ? v : "<missing>";
        }
        catch (Exception e)
        {
            return "<" + e.Message + ">";
        }
    }

    private static bool SetServerSideReset(DefaultMQAdminExt admin, string brokerAddr,
        string value)
    {
        try
        {
            var props = new PropertyMap { [SwitchKey] = value };
            admin.UpdateBrokerConfig(brokerAddr, props);
        }
        catch (Exception e)
        {
            Console.WriteLine("  [diag] updateBrokerConfig(" + SwitchKey + "=" + value
                + ") failed: " + e.Message);
            return false;
        }

        Thread.Sleep(1000);
        return ReadFlag(admin, brokerAddr, SwitchKey) == value;
    }

    public static int Run(string[] args)
    {
        if (args.Length > 0 && args[0].Trim().Length > 0) _namesrv = args[0].Trim();
        string stamp = (NowMs() % 1000000).ToString(CultureInfo.InvariantCulture);
        string backTopic = "ResetOffsetBack_" + stamp;
        string skipTopic = "ResetOffsetSkip_" + stamp;
        string backGroup = "GID_ResetOffsetBack_" + stamp;
        string skipGroup = "GID_ResetOffsetSkip_" + stamp;
        Console.WriteLine("namesrv=" + _namesrv + " topic1=" + backTopic + " topic2=" + skipTopic);

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

        var setup = new MQClientInstance("rosetup-" + stamp, new List<string> { _namesrv });
        setup.Start();
        var producer = new DefaultMQProducer("ResetOffsetLive_pg_" + stamp)
        {
            NamesrvAddr = _namesrv,
            InstanceName = "reset-offset-live-" + stamp,
        };

        try
        {
            // 开关热改：useServerSideResetOffset=false（220 推送路径的前提）
            Check("开关热改：useServerSideResetOffset=false（220 推送路径的前提）",
                SetServerSideReset(admin, brokerAddr, "false"),
                "回读 " + SwitchKey + "=" + ReadFlag(admin, brokerAddr, SwitchKey));

            // 显式 1 队列建 topic：S1 的重投序列（3..14）与 S2 的前跳都要可数的队列数
            setup.CreateTopicInRoute(backTopic, 1, 1);
            setup.CreateTopicInRoute(skipTopic, 1, 1);
            producer.Start();
            Thread.Sleep(3000);

            ScenarioBackwardReset(producer, admin, brokerAddr, backTopic, backGroup, stamp);
            ScenarioForwardSkip(producer, admin, brokerAddr, skipTopic, skipGroup, stamp);
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

            foreach (string t in new[] { backTopic, skipTopic })
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

            foreach (string g in new[] { backGroup, skipGroup })
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
                Check("还原 useServerSideResetOffset=true",
                    SetServerSideReset(admin, brokerAddr, "true"),
                    "回读 " + SwitchKey + "=" + ReadFlag(admin, brokerAddr, SwitchKey));
            }
            catch (Exception e)
            {
                Check("还原 useServerSideResetOffset=true", false, e.Message);
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

    private static void ScenarioBackwardReset(DefaultMQProducer producer, DefaultMQAdminExt admin,
        string brokerAddr, string topic, string group, string stamp)
    {
        Console.WriteLine("=== S1 回退重置（10 → 3）：立刻落盘 + 在途批次整批作废 ===");

        // 前置：先用普通 listener 把 broker 上的位点做成 10（reset 要求该组在 broker 上有记录）
        var seed = new RecordingListener();
        var c0 = new DefaultMQPushConsumer(group)
        {
            InstanceName = "reset-offset-back-seed-" + stamp,
            ConsumeMessageBatchMaxSize = 1,
            ConsumeFromWhere = RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        c0.SetNamesrvAddr(_namesrv);
        c0.SetMessageListener(seed);
        c0.Subscribe(topic, "*");
        c0.Start();

        MessageQueue mq;
        try
        {
            List<MessageQueue> queues = WaitForSingleQueue(c0, topic, "S1");
            if (queues.Count != 1) return;
            mq = queues[0];

            long mid = 0;
            for (int i = 0; i < Msgs; ++i)
            {
                producer.Send(new Message(topic, Str2Bytes("rb-" + N(i))));
                if (i == 2)
                {
                    mid = NowMs();
                    // 让第 3 条（offset 3）与前面三条拉开存储时间，后面按时间戳重置才能稳定命中 3
                    Thread.Sleep(2000);
                }
            }

            bool seeded = WaitUntil(() => seed.Offsets().Count == Msgs, 30000, 200);
            Check("S1-前置：消费者消费掉 " + N(Msgs) + " 条", seeded,
                "arrivals=" + Join(seed.Offsets()));

            bool primed = WaitUntil(() => AllBack(admin, group, mq, Msgs), 20000, 500);
            Check("S1-前置：broker 位点周期落盘到 " + N(Msgs) + "（reset 要求组在 broker 上有记录）",
                primed, "committed=" + N(Probe(admin, group, mq).Committed));
            c0.Shutdown();
            Thread.Sleep(1000);

            // 主力消费者：逐批闸住 + 周期落盘 60s ⇒ 窗口内唯一能改 broker 位点的路径是重置自带的那次
            var listener = new SteppingListener();
            var c = new DefaultMQPushConsumer(group)
            {
                InstanceName = "reset-offset-back-" + stamp,
                ConsumeMessageBatchMaxSize = 1,
                PersistConsumerOffsetIntervalMillis = 60000,
                ConsumeFromWhere =
                    RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset,
            };
            c.SetNamesrvAddr(_namesrv);
            c.SetMessageListener(listener);
            c.Subscribe(topic, "*");
            long tStart = NowMs();
            c.Start();

            try
            {
                Thread.Sleep(2000);
                for (int i = Msgs; i < Msgs + 5; ++i)
                {
                    producer.Send(new Message(topic, Str2Bytes("rb-" + N(i))));
                }

                string key = Key(mq);
                int pending = -1;
                bool arranged = WaitUntil(() =>
                {
                    pending = c.PendingForTest(key).Count;
                    return listener.BatchCount() >= 1 && pending == 4;
                }, 30000, 50);
                List<long> arr = listener.Offsets();
                Check("S1-窗口就绪：1 条在途（offset 10 卡在 listener）+ 4 条留在缓冲",
                    arranged && arr.Count > 0 && arr[0] == Msgs,
                    "arrivals=" + Join(arr) + " pending=" + N(pending));
                if (!arranged) return;

                // 等周期落盘的**首跳**过去（Java initialDelay＝start 后 10s，之后才是 60s 周期）。
                // 不等它，重置后那次 persist 会与首跳混在一起，"broker 位点之所以是 3" 就说不清。
                WaitUntil(() => NowMs() - tStart > 12000, 15000, 200);
                Check("S1-首跳周期落盘已过（此后 60s 内不再有周期写）",
                    NowMs() - tStart > 12000 && AllBack(admin, group, mq, Msgs),
                    "committed=" + N(Probe(admin, group, mq).Committed));

                // 按时间戳重置到 3：mid 落在第 3 条与第 4 条之间 ⇒ getOffsetInQueueByTime 命中 3，
                // 3 < consumerOffset(10) 且 isForce=true ⇒ broker 推 {mq: 3}
                long resetTs = mid + 500;
                long t0 = NowMs();
                SortedDictionary<MessageQueue, long> table =
                    admin.ResetOffsetByTimestamp(topic, group, resetTs, true);
                Check("S1-222 响应里的目标位点就是 3",
                    table.Count == 1 && table.Values.First() == BackTarget,
                    "table={" + string.Join(", ", table.Select(kv => "q" + N(kv.Key.QueueId)
                        + ":" + N(kv.Value))) + "}");

                bool in2s = WaitBefore(() => AllBack(admin, group, mq, BackTarget), 2000);
                Check("S1-broker 位点 ~2s 内变成 3（重置路径自带的那次 persist，周期落盘=60s）", in2s,
                    "elapsed=" + ((NowMs() - t0) / 1000.0).ToString("F1", CultureInfo.InvariantCulture)
                    + "s committed=" + N(Probe(admin, group, mq).Committed));

                long? off = c.ConsumeOffsetForTest(key);
                Check("S1-重置后本地表里没有旧位点（Java removeOffset；新位点经撤销尾巴出去）",
                    off == null, "local_offset=" + (off.HasValue ? N(off.Value) : "None")
                    + " epoch=" + N(c.QueueEpochForTest(key)));

                // 放行旧批次：它的 ack 属于已被撤销的 ProcessQueue，必须作废
                listener.Release();
                bool second = WaitBatches(listener, 2, 20000);
                off = c.ConsumeOffsetForTest(key);
                Check("S1-旧批次 ack 作废（放行后新队列第一批 offset 3 已在途时，本地位点仍未越过 3）",
                    second && (off == null || off.Value <= BackTarget),
                    "local_offset=" + (off.HasValue ? N(off.Value) : "None")
                    + " arrivals=" + Join(listener.Offsets()));

                // 逐批放行走完重投：3..14 每条重投一次（旧缓冲里 11..14 也已作废，只能作为重投出现）
                var expected = new List<long> { Msgs };
                for (long i = BackTarget; i < Msgs + 5; ++i) expected.Add(i);
                for (int n = 2; n <= expected.Count; ++n)
                {
                    listener.Release();
                    if (n < expected.Count) WaitBatches(listener, n + 1, 15000);
                }

                arr = listener.Offsets();
                Check("S1-队列被真正重建：重投序列是 " + Join(expected), arr.SequenceEqual(expected),
                    "arrivals=" + Join(arr));
                WaitUntil(() => c.ConsumeOffsetForTest(key) == Msgs + 5, 10000, 50);

                Check("S1-窗口内只有重置那次写 broker（位点仍停在 3，周期落盘=60s）",
                    AllBack(admin, group, mq, BackTarget),
                    "committed=" + N(Probe(admin, group, mq).Committed));

                // 关停会同步落盘一次：重投的 ack 才是最终值（15 = 最后一条 14 的 +1）
                c.Shutdown();
                bool advanced = WaitUntil(() => AllBack(admin, group, mq, Msgs + 5), 15000, 500);
                Check("S1-关停落盘把重投的 ack 写回 broker（位点前进到 15）", advanced,
                    "committed=" + N(Probe(admin, group, mq).Committed));
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
        finally
        {
            try
            {
                c0.Shutdown();
            }
            catch (Exception)
            {
                // 收尾失败不掩盖主断言
            }
        }
    }

    private static void ScenarioForwardSkip(DefaultMQProducer producer, DefaultMQAdminExt admin,
        string brokerAddr, string topic, string group, string stamp)
    {
        Console.WriteLine("=== S2 前跳重置（timestamp=-1 → maxOffset）不重投 + 新消息照常消费 ===");

        var listener = new SteppingListener();
        // 周期落盘用默认值：本场景要先靠首跳（start 后 10s）在 broker 上给这个组建记录
        //（Broker2Client.resetOffset 对 queryOffset==-1 的组直接回 SYSTEM_ERROR），
        // 再等一个 >5s 的窗口做重置 —— 重置那笔 persist 与周期写不同刻，判据仍然干净。
        var c = new DefaultMQPushConsumer(group)
        {
            InstanceName = "reset-offset-skip-" + stamp,
            ConsumeMessageBatchMaxSize = 1,
            ConsumeFromWhere = RocketMQ.Remoting.Protocol.ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        c.SetNamesrvAddr(_namesrv);
        c.SetMessageListener(listener);
        c.Subscribe(topic, "*");
        long tStart = NowMs();
        c.Start();

        try
        {
            List<MessageQueue> queues = WaitForSingleQueue(c, topic, "S2");
            if (queues.Count != 1) return;
            MessageQueue mq = queues[0];

            Thread.Sleep(2000);
            for (int i = 0; i < Msgs; ++i)
            {
                producer.Send(new Message(topic, Str2Bytes("rs-" + N(i))));
            }

            // 逐批放行前 3 条：第 4 条（offset 3）留在 listener 里当"在途批次"
            for (int n = 1; n <= 4; ++n)
            {
                if (!WaitBatches(listener, n, 30000)) break;
                if (n < 4) listener.Release();
            }

            List<long> arr = listener.Offsets();
            var seedArrivals = new List<long> { 0, 1, 2, 3 };
            bool arranged = listener.BatchCount() >= 4 && arr.SequenceEqual(seedArrivals);
            Check("S2-窗口就绪：前 3 条已 ack、第 4 条卡在 listener", arranged,
                "arrivals=" + Join(arr));
            if (!arranged) return;

            // 等首跳落盘把组建出来（本地已消费位点 3 = 前三条的 ack），并留出 >5s 的静默窗口
            bool primed = WaitUntil(
                () => NowMs() - tStart > 12000 && AllBack(admin, group, mq, BackTarget), 20000, 500);
            Check("S2-前置：broker 上该组有记录（q0:3），且下一笔周期写还在 5s 之外", primed,
                "committed=" + N(Probe(admin, group, mq).Committed));

            long t0 = NowMs();
            SortedDictionary<MessageQueue, long> table =
                admin.ResetOffsetByTimestamp(topic, group, -1, true);
            Check("S2-222 响应里的目标位点就是 maxOffset(10)",
                table.Count == 1 && table.Values.First() == Msgs,
                "table={" + string.Join(", ", table.Select(kv => "q" + N(kv.Key.QueueId) + ":"
                    + N(kv.Value))) + "}");

            bool in2s = WaitBefore(() => AllBack(admin, group, mq, Msgs), 2000);
            Check("S2-broker 位点 ~2s 内前跳到 10（重置路径自带的那次 persist）", in2s,
                "elapsed=" + ((NowMs() - t0) / 1000.0).ToString("F1", CultureInfo.InvariantCulture)
                + "s committed=" + N(Probe(admin, group, mq).Committed));

            listener.Release();
            Thread.Sleep(3000);
            arr = listener.Offsets();
            Check("S2-被跳过的 4..9 一条都不投（在途那条的 ack 也作废）",
                arr.SequenceEqual(seedArrivals), "arrivals=" + Join(arr));

            producer.Send(new Message(topic, Str2Bytes("rs-new")));
            bool gotNew = WaitBatches(listener, 5, 20000);
            listener.Release();
            arr = listener.Offsets();
            var wantArrivals = new List<long> { 0, 1, 2, 3, Msgs };
            Check("S2-重建后的队列从队尾续跑（新消息 offset 10 正常投递）",
                gotNew && arr.SequenceEqual(wantArrivals), "arrivals=" + Join(arr));

            // Release() 只让 listener 返回；ack 是消费线程随后落的。不等本地位点真的推到 11 就
            // 关停，关停那次 persist 可能跑在 ack 之前（写回的还是 10）——这是断言竞态，不是语义问题。
            string key = Key(mq);
            WaitUntil(() => c.ConsumeOffsetForTest(key) == Msgs + 1, 10000, 50);
            c.Shutdown();
            bool advanced = WaitUntil(() => AllBack(admin, group, mq, Msgs + 1), 15000, 500);
            Check("S2-关停落盘把新消息的 ack 写回 broker（位点前进到 11）", advanced,
                "committed=" + N(Probe(admin, group, mq).Committed));
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

    private static List<MessageQueue> WaitForSingleQueue(DefaultMQPushConsumer c, string topic,
        string tag)
    {
        List<MessageQueue> queues = new();
        bool one = WaitUntil(() =>
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
        Check(tag + "-topic 恰好 1 个队列", one, "queues=" + N(queues.Count));
        return queues;
    }

    private static int Report()
    {
        Console.WriteLine();
        Console.WriteLine("=== 结果：" + N(_pass) + " PASS / " + N(_fail) + " FAIL ===");
        return _fail == 0 ? 0 : 1;
    }
}
