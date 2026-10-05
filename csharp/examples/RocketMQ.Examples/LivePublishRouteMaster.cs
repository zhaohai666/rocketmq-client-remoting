// 发布路由必须跳过「没有 master 的 broker」真机验证（Java MQClientInstance:294-303）。
//（对应 python/verify_publish_route_master_live.py、cpp/examples/live_publish_route_master.cpp、
//  rust/examples/live_publish_route_master.rs）
//
// 前置：namesrv + master + slave 都在跑（按本地集群 runbook）；脚本自己**只停一次 master**
// （scripts/rmq_test_broker.sh 只认 master 的 java 进程），收尾无条件把它拉回来。
//
// Java 依据：MQClientInstance.topicRouteData2TopicPublishInfo:294-303 组装发布信息时，
// brokerDatas 里没有同名 broker、或它的 brokerAddrs 没有 MASTER_ID，整条 QueueData 跳过。
// 从节点自己也注册进 namesrv，且默认配置下照样带写位（RouteInfoManager 只在「prime slave
// 且 enableActingMaster」时才抹掉 WRITE，本机 broker.conf 是 false），所以 master 一掉线，
// 路由里同一个 brokerName 只剩 brokerId=1 —— 漏判这条，生产者就会把消息发到从节点上，
// 而从节点对发送请求一律 reject（SendMessageProcessor ⇒ SYSTEM_BUSY(2)，**还是可重试码**），
// 白烧重试。消费侧是另一份口径（topicRouteData2TopicSubscribeInfo:318-332：读位 +
// readQueueNums、**不要求有 master**），停窗口内消费者仍要看得见队列、还得能从从节点拉。
//
// 离线单测（tests/RouteHeartbeatTests.cs）锁的是判据；真机锁的是判据作用在真实路由形状上的
// 结果 —— 名字服务里 broker-a 真的只剩 {1: slave}。
//
// 场景（同一停窗口里做完）：
//   S0 控制腿（master 在）：路由 {0: master, 1: slave}；发布队列 4、订阅队列 4。
//   S1 预埋：每队列定点一条共 4 条，等从节点 store 追上（不然 S6 无从消费）。
//   S2 停 master → 刷新路由直到 broker-a 只剩 {1: slave}。
//   S3 (A) 发布信息组不出队列：GetTopicPublishInfo 只在**有队列**时才返回，此刻抛
//      「Can not find Message Queue for topic」（等价于 Python 读表得到的 msg_queue_list == 0，
//      且多证一次「重新拉回来的路由照样组不出队列」——访问器会先刷一次路由）。
//   S4 (C) 订阅队列仍是 4（消费侧不看 master）。
//   S5 发送快速失败、报错里没有从节点地址（旧缓存腿打的是死掉的 master；周期刷新恰好已跑过
//      则是本端 10005）；再显式把发送实例刷成停后形状：(A) 生效 —— 发送本端 10005 且
//      一条 broker wire 都不发（无 BrokersSent）。
//   S5d (B) 对照：定点发到该队列 → 地址侧只认 master（Java findBrokerAddressInPublish:1295-1305），
//      本端同样立刻报「The broker[broker-a] not exist」，也无 wire（漏掉 (B) 这条对照时的旧行为：
//      请求打到从节点上，broker 回 SYSTEM_BUSY(2)，一个可重试码 —— 白烧一整轮重试，错误类型也和
//      Java 不一样）。两条腿合起来是「无 wire」，落库与否由 S7b 钉死。
//   S5e (D) 订阅口径：顺序锁整台跳过（RebalanceImpl#lock:153/lockAll:195 只认主、不刷路由；
//      对照腿直接点名从节点，证明从节点**本来**发得出锁 —— 空集是客户端没去，不是 broker 拒绝）。
//   S5f (E) 订阅口径：POP 本端报「broker 不存在」（PullAPIWrapper#popAsync:369-373），不发 wire。
//   S5g (F) 位点读取：冷实例（缓存里没这个 topic）刷一次路由后**放宽**到从节点
//      （RemoteBrokerOffsetStore#fetchConsumeOffsetFromBroker:237-241）。
//   S6 (C 端到端) 停窗口内新起的 push 消费者仍看到 4 条队列，并从**从节点**把 S1 的 4 条收齐。
//   S7 负控：master 拉回 → 发布队列恢复 4、两条失败发送都没在 broker 上留下消息、发送 SEND_OK。
//
// 用法：rmq publish-route-master [namesrv] [master] [slave]
using System.Diagnostics;
using System.Globalization;
using System.Text;

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

namespace RocketMQ.Examples;

public static class LivePublishRouteMaster
{
    private const int Queues = 4;
    private const string BrokerName = "broker-a";

    /// <summary>本端失败上界：发布信息为空时压根没有 broker wire 调用，真机给 500ms 已留量级余量。</summary>
    private const double LocalBudgetMs = 500.0;

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

    private static string ScriptPath()
    {
        string root = Environment.GetEnvironmentVariable("RMQ_REPO_ROOT") ?? "..";
        return Path.Combine(root, "scripts", "rmq_test_broker.sh");
    }

    /// <summary>
    /// 跑 scripts/rmq_test_broker.sh（**只碰 master**，四个语言的 live 用例共用）。
    /// 输出走**文件**而不是管道：start 会把 broker 拉成常驻进程，谁继承它的写端谁就等不到 EOF。
    /// </summary>
    private static (bool Ok, string Out) BrokerCtl(string script, string action)
    {
        string logPath = "/tmp/rmq_pr_master_cs_broker_ctl." + action + ".log";
        try
        {
            var psi = new ProcessStartInfo("/bin/sh",
                "-c \"sh '" + script + "' " + action + " > '" + logPath + "' 2>&1 < /dev/null\"")
            {
                UseShellExecute = false,
            };
            using Process? proc = Process.Start(psi);
            proc?.WaitForExit();
            string text = File.Exists(logPath) ? File.ReadAllText(logPath).Trim() : string.Empty;
            bool ok = proc is not null && proc.ExitCode == 0;
            return (ok, text.Length > 0 ? text : "exit=" + (proc?.ExitCode ?? -1));
        }
        catch (Exception ex)
        {
            return (false, "run " + action + " failed: " + ex.Message);
        }
    }

    /// <summary>真机投递/复制受 broker 长轮询与同机负载影响，固定 sleep 会把「实现没问题」测成假失败。</summary>
    private static bool WaitUntil(Func<bool> pred, int timeoutMs, int intervalMs = 500)
    {
        var sw = Stopwatch.StartNew();
        while (sw.ElapsedMilliseconds < timeoutMs)
        {
            if (pred())
            {
                return true;
            }

            Thread.Sleep(intervalMs);
        }

        return pred();
    }

    private static string FmtAddrs(SortedDictionary<long, string> addrs) =>
        "{" + string.Join(", ", addrs.Select(kv => kv.Key + ": " + kv.Value)) + "}";

    private static string FmtQueues(List<MessageQueue> queues) =>
        "[" + string.Join(", ", queues.Select(q => q.BrokerName + ":" + q.QueueId)) + "]";

    private static string FmtOffsets(List<(int QueueId, long Offset)> rows) =>
        "[" + string.Join(", ", rows.Select(r => "q" + r.QueueId + "=" + r.Offset)) + "]";

    /// <summary>只收 body：这一趟关心的是「从节点上的 4 条能不能收齐」，不关心位点与顺序。</summary>
    private sealed class Sink
    {
        private readonly object _lock = new();
        private readonly List<string> _bodies = new();

        public void Add(string body)
        {
            lock (_lock)
            {
                _bodies.Add(body);
            }
        }

        public List<string> Bodies()
        {
            lock (_lock)
            {
                return new List<string>(_bodies);
            }
        }
    }

    private sealed class BodyListener : IMessageListenerConcurrently
    {
        private readonly Sink _sink;

        public BodyListener(Sink sink) => _sink = sink;

        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
        {
            foreach (MessageExt m in msgs)
            {
                _sink.Add(Encoding.UTF8.GetString(m.Body));
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    private sealed class Fixture
    {
        public string Namesrv = string.Empty;
        public string Master = string.Empty;
        public string Slave = string.Empty;
        public string Stamp = string.Empty;
        public string Topic = string.Empty;
        public DefaultMQAdminExt Admin = null!;
        public DefaultMQProducer Producer = null!;
        public DefaultMQPushConsumer? Consumer;

        public void Start(string namesrv, string master, string slave)
        {
            Namesrv = namesrv;
            Master = master;
            Slave = slave;
            Stamp = DateTimeOffset.UtcNow.ToUnixTimeMilliseconds()
                .ToString(CultureInfo.InvariantCulture);
            Topic = "PrMasterCs" + Stamp;

            Admin = new DefaultMQAdminExt();
            Admin.SetNamesrvAddr(namesrv);
            Admin.Start();

            Producer = new DefaultMQProducer("GID_PrMasterCsPg_" + Stamp)
            {
                NamesrvAddr = namesrv,
                InstanceName = "pr_master_cs_p_" + Stamp,
            };
            Producer.Start();
        }

        /// <summary>broker-a 当前的 brokerAddrs（每次调用都强制刷一次路由，等停/恢复靠轮询它）。</summary>
        public SortedDictionary<long, string> RouteAddrs()
        {
            Admin.Client().UpdateTopicRouteInfoFromNameServer(Topic, false, 5000);
            TopicRouteData? route = Admin.Client().GetTopicRouteData(Topic);
            BrokerData? bd = route?.BrokerDatas.FirstOrDefault(b => b.BrokerName == BrokerName);
            return bd is null
                ? new SortedDictionary<long, string>()
                : new SortedDictionary<long, string>(bd.BrokerAddrs);
        }

        public List<MessageQueue> SeedQueues() =>
            Admin.Client().GetTopicPublishInfo(Topic, false).MsgQueueList;

        public long MaxOffset(MessageQueue mq, string? addr = null) =>
            Admin.Client().GetMaxOffset(mq, 5000, addr);

        public DefaultMQPushConsumer BuildConsumer(string group, Sink sink)
        {
            var c = new DefaultMQPushConsumer(group);
            c.SetNamesrvAddr(Namesrv);
            c.InstanceName = "pr_master_cs_c_" + Stamp;
            c.ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset;
            c.ConsumeMessageBatchMaxSize = 3;
            c.SetMessageListener(new BodyListener(sink));
            c.Subscribe(Topic, "*");
            return c;
        }

        /// <summary>收尾：先把 master 拉回来（start 幂等），再删 topic、关客户端 —— 不能留一份孤儿配置。</summary>
        public void Cleanup(string script)
        {
            BrokerCtl(script, "start");
            foreach (string addr in new[] { Master, Slave })
            {
                try
                {
                    Admin.Client().DeleteTopicInBroker(addr, Topic, 5000);
                }
                catch (Exception e)
                {
                    Console.WriteLine("    (delete topic on " + addr + " 失败: " + e.Message + ")");
                }
            }

            try
            {
                Admin.Client().DeleteTopicInNamesrv(Topic, 5000);
            }
            catch (Exception e)
            {
                Console.WriteLine("    (delete topic in namesrv 失败: " + e.Message + ")");
            }

            try
            {
                Consumer?.Shutdown();
            }
            catch (Exception)
            {
                // 收尾失败不该掩盖用例结论
            }

            try
            {
                Producer.Shutdown();
            }
            catch (Exception)
            {
                // 同上
            }

            try
            {
                Admin.Shutdown();
            }
            catch (Exception)
            {
                // 同上
            }
        }
    }

    /// <summary>发布信息为空 ⇒ GetTopicPublishInfo 抛「Can not find Message Queue for topic」。</summary>
    private static bool PublishInfoEmpty(MQClientInstance client, string topic, out string detail)
    {
        try
        {
            TopicPublishInfo info = client.GetTopicPublishInfo(topic, false);
            detail = "意外拿到 " + info.MsgQueueList.Count + " 条队列";
            return false;
        }
        catch (MQClientException e)
        {
            detail = e.Message;
            return e.Message.Contains("Can not find Message Queue for topic", StringComparison.Ordinal);
        }
    }

    private static void Scenario(Fixture fx, string script)
    {
        string topic = fx.Topic;

        // ---------- S0 控制腿 ----------
        Console.WriteLine("S0 控制腿（master 在）：路由 {0: master, 1: slave}、发布/订阅各 " + Queues + " 条");
        try
        {
            fx.Admin.Client().CreateTopicInRoute(topic, Queues, Queues, MixAll.ReadPermByDefault, 5000);
        }
        catch (Exception e)
        {
            Check("S0 建 topic（master）", false, e.Message);
            return;
        }

        // 从节点也直建一份，不赌 SlaveSynchronize 的 5s 周期
        try
        {
            fx.Admin.CreateTopicInBroker(fx.Slave, topic, Queues, Queues, MixAll.ReadPermByDefault);
        }
        catch (Exception e)
        {
            Console.WriteLine("    (从节点建 topic 失败: " + e.Message + ")");
        }

        SortedDictionary<long, string> addrs = new();
        bool routeUp = WaitUntil(() =>
        {
            addrs = fx.RouteAddrs();
            return addrs.ContainsKey(0) && addrs.ContainsKey(1);
        }, 30000);
        Check("S0 路由含 {0: master, 1: slave}", routeUp, "broker_addrs=" + FmtAddrs(addrs));
        if (!routeUp)
        {
            return;
        }

        string routeSlave = addrs.TryGetValue(1, out string? s1) ? s1 : string.Empty;
        Check("S0 从节点地址与参数一致", routeSlave == fx.Slave,
            "route=" + routeSlave + " argv=" + fx.Slave);

        List<MessageQueue> queues;
        try
        {
            queues = fx.SeedQueues();
        }
        catch (Exception e)
        {
            Check("S0 发布队列 " + Queues + "（控制）", false, e.Message);
            return;
        }

        queues = queues.OrderBy(q => q.QueueId).ToList();
        Check("S0 发布队列 " + Queues + "（控制）", queues.Count == Queues,
            "queues=" + FmtQueues(queues));
        List<MessageQueue> subs = fx.Admin.Client().GetTopicSubscribeInfo(topic);
        Check("S0 订阅队列 " + Queues + "（控制）", subs.Count == Queues, "queues=" + FmtQueues(subs));
        if (queues.Count != Queues)
        {
            return;
        }

        // ---------- S1 预埋 ----------
        Console.WriteLine();
        Console.WriteLine("S1 预埋 " + Queues + " 条（每队列定点一条）并等从节点 store 追上");
        var seeded = new List<string>();
        for (int i = 0; i < queues.Count; i++)
        {
            string body = "pr-master-" + i.ToString(CultureInfo.InvariantCulture);
            var msg = new Message(topic, Bytes(body));
            try
            {
                SendResult r = fx.Producer.Send(msg, queues[i], 20000);
                if (r.SendStatus == SendStatus.SendOk)
                {
                    seeded.Add(body);
                }
                else
                {
                    Check("S1 第 " + i + " 条预埋 SEND_OK", false, "status=" + r.SendStatus);
                }
            }
            catch (Exception e)
            {
                Check("S1 第 " + i + " 条预埋 SEND_OK", false, e.Message);
            }
        }

        Check("S1 " + Queues + " 条预埋全部 SEND_OK", seeded.Count == Queues, "seeded=" + seeded.Count);
        if (seeded.Count != Queues)
        {
            return;
        }

        var masterMax = new List<(int QueueId, long Offset)>();
        foreach (MessageQueue mq in queues)
        {
            try
            {
                masterMax.Add((mq.QueueId, fx.MaxOffset(mq)));
            }
            catch (Exception e)
            {
                Console.WriteLine("    (master maxOffset 查询失败: " + e.Message + ")");
            }
        }

        bool caughtUp = WaitUntil(() =>
        {
            if (masterMax.Count == 0)
            {
                return false;
            }

            var slaveMax = new List<(int QueueId, long Offset)>();
            foreach (MessageQueue mq in queues)
            {
                try
                {
                    slaveMax.Add((mq.QueueId, fx.MaxOffset(mq, fx.Slave)));
                }
                catch (Exception e)
                {
                    Console.WriteLine("    (从节点取 maxOffset 失败: " + e.Message + ")");
                    return false;
                }
            }

            return masterMax.All(m => slaveMax.Any(s => s.QueueId == m.QueueId && s.Offset >= m.Offset));
        }, 30000);
        Check("S1 " + Queues + " 条已复制到从节点 store", caughtUp,
            "master=" + FmtOffsets(masterMax));

        // ---------- S2 停 master ----------
        Console.WriteLine();
        Console.WriteLine("S2 停 master（scripts/rmq_test_broker.sh stop），等路由只剩从节点");
        (bool stopped, string stopOut) = BrokerCtl(script, "stop");
        Check("S2 master 已优雅停机", stopped, stopOut);

        SortedDictionary<long, string> downAddrs = new();
        bool masterless = WaitUntil(() =>
        {
            downAddrs = fx.RouteAddrs();
            return !downAddrs.ContainsKey(0) && downAddrs.ContainsKey(1);
        }, 60000);
        Check("S2 路由里 broker-a 只剩 {1: slave}", masterless, "broker_addrs=" + FmtAddrs(downAddrs));
        if (!masterless)
        {
            return;
        }

        // ---------- S3 (A) 发布信息组不出队列 ----------
        Console.WriteLine();
        Console.WriteLine("S3 (A) 发布信息跳过没有 master 的 broker");
        bool s3 = PublishInfoEmpty(fx.Admin.Client(), topic, out string s3Detail);
        Check("S3 停 master 后发布队列 == 0（访问器本端抛「选不到队列」）", s3, s3Detail);

        // ---------- S4 (C) 订阅队列 ----------
        List<MessageQueue> subsDown = fx.Admin.Client().GetTopicSubscribeInfo(topic);
        Check("S4 (C) 订阅队列仍是 " + Queues + "、且都在 " + BrokerName + "（消费侧不看 master）",
            subsDown.Count == Queues && subsDown.All(q => q.BrokerName == BrokerName),
            "queues=" + FmtQueues(subsDown));

        // ---------- S5 (A 定型) 发送快速失败 ----------
        // 分两段看：**缓存还没刷**时（现实中 30s 周期任务未到）发送实例手里还是停前的旧路由，
        // 地址解析落在死掉的 master 上，快速失败、绝不静默改发从节点；**路由刷成停后形状**后
        // （周期任务 / 显式刷新），(A) 生效：发布队列为空，发送连一条 broker wire 都不发。
        Console.WriteLine();
        Console.WriteLine("S5 不指定队列的同步发送：旧缓存快速失败 → 刷新后本端快速失败");
        var sw = Stopwatch.StartNew();
        string staleText;
        bool staleFailed;
        try
        {
            SendResult r = fx.Producer.Send(new Message(topic, Bytes("must-not-send")), 20000);
            staleText = "SendResult(status=" + r.SendStatus + ")";
            staleFailed = false;
        }
        catch (Exception e)
        {
            staleText = e.GetType().Name + ": " + e.Message;
            staleFailed = true;
        }

        double staleMs = sw.Elapsed.TotalMilliseconds;
        // 这一腿是机会腿：周期刷新是否已经跑过不由本脚本定。两条腿的共同判据是
        // "快速失败 + 绝不落到从节点地址上"（旧缓存腿打的是死掉的 master）。
        Check("S5 发送快速失败，且报错里没有从节点地址（绝不改发从节点）",
            staleFailed && staleMs < LocalBudgetMs && !staleText.Contains(fx.Slave, StringComparison.Ordinal),
            staleMs.ToString("F0", CultureInfo.InvariantCulture) + "ms " + staleText);

        // 让发送实例自己的路由缓存刷成停后形状（与 30s 周期任务同一条代码路径）
        MQClientInstance pcli;
        try
        {
            pcli = fx.Producer.Client();
        }
        catch (Exception e)
        {
            Check("S5b 拿到发送实例", false, e.Message);
            return;
        }

        try
        {
            pcli.UpdateTopicRouteInfoFromNameServer(topic, false, 5000);
        }
        catch (Exception e)
        {
            Console.WriteLine("    (发送实例路由刷新失败: " + e.Message + ")");
        }

        bool s5b = PublishInfoEmpty(pcli, topic, out string s5bDetail);
        Check("S5b 发送实例的发布队列也 == 0（(A) 就作用在这里）", s5b, s5bDetail);

        sw.Restart();
        string freshText;
        bool freshFailed = false;
        int freshCode = -1;
        try
        {
            SendResult r = fx.Producer.Send(new Message(topic, Bytes("must-not-send-2")), 20000);
            freshText = "SendResult(status=" + r.SendStatus + ")";
        }
        catch (MQClientException e)
        {
            freshFailed = true;
            freshCode = e.ResponseCode;
            freshText = e.Message;
        }
        catch (Exception e)
        {
            freshFailed = true;
            freshText = e.GetType().Name + ": " + e.Message;
        }

        double freshMs = sw.Elapsed.TotalMilliseconds;
        Check("S5c 刷新后：本端 10005 抛「选不到队列」，无 wire 调用（无 BrokersSent）",
            freshFailed
            && freshCode == ClientErrorCode.NotFoundTopicException
            && freshText.Contains("Can not find Message Queue for topic", StringComparison.Ordinal)
            && !freshText.Contains("BrokersSent", StringComparison.Ordinal)
            && freshMs < LocalBudgetMs,
            freshMs.ToString("F0", CultureInfo.InvariantCulture) + "ms code=" + freshCode + ": " + freshText);

        // ---------- S5d (B) 对照：定点发送的地址解析 ----------
        // 定点发送不经过发布信息（调用方直接给了 mq），地址侧若还按「主优先、没主退一台」去解析，
        // 请求就会落到从节点上换来一个 SYSTEM_BUSY(2)。Java 的 findBrokerAddressInPublish
        // 只认 brokerId=0，本端应当直接报「broker 不存在」，一条 wire 都不发。
        Console.WriteLine();
        Console.WriteLine("S5d (B) 对照：定点发到该队列 → 本端报 broker 不存在（不发 wire）");
        var mq0 = new MessageQueue(topic, BrokerName, 0);
        int pinnedCode = -1;
        bool pinnedFailed = false;
        bool pinnedIsClientError = false;
        string pinnedText;
        sw.Restart();
        try
        {
            SendResult r = fx.Producer.Send(new Message(topic, Bytes("pinned-to-slave")), mq0, 20000);
            pinnedText = "SendResult(status=" + r.SendStatus + ")";
        }
        catch (MQClientException e)
        {
            // MQBrokerException 不继承 MQClientException：这里能进来说明不是 broker 回的码
            pinnedFailed = true;
            pinnedIsClientError = true;
            pinnedCode = e.ResponseCode;
            pinnedText = e.Message;
        }
        catch (Exception e)
        {
            pinnedFailed = true;
            pinnedText = e.GetType().Name + ": " + e.Message;
        }

        double pinnedMs = sw.Elapsed.TotalMilliseconds;
        Check("S5d 定点发送本端报「The broker[" + BrokerName + "] not exist」，无 wire"
              + "（不是从节点回的 SYSTEM_BUSY(2)）",
            pinnedFailed && pinnedIsClientError && pinnedCode == -1
            && pinnedText == "The broker[" + BrokerName + "] not exist"
            && pinnedMs < LocalBudgetMs,
            pinnedMs.ToString("F0", CultureInfo.InvariantCulture) + "ms code=" + pinnedCode
            + ": " + pinnedText);

        // ---------- S5e (D) 订阅口径：顺序锁只认主 ----------
        // Java RebalanceImpl#lock:153 / lockAll:195 走 findBrokerAddressInSubscribe(brokerName,
        // MASTER_ID, true)：只认主、**不刷路由**，拿不到就整台跳过。退到从节点上锁等于锁在
        // 从节点的锁管理器里，master 不知情，顺序消费的互斥静默失效。此刻 admin 实例的路由
        // 缓存已被 S2 刷成 masterless 形状，任何「退让」口径都会落到从节点上并拿回非空锁集，
        // 所以空集 + S5e2 对照腿足以说明客户端压根没去。
        Console.WriteLine();
        Console.WriteLine("S5e (D) 顺序锁：停窗口内整台跳过（不刷路由、不发 wire）");
        string group = "GID_PrMasterCs_" + fx.Stamp;
        string clientId = "pr_master_cs_lock_" + fx.Stamp;
        var lockMqs = new List<MessageQueue> { new(topic, BrokerName, 0) };
        List<MessageQueue> locks = new();
        sw.Restart();
        try
        {
            locks = fx.Admin.Client().LockBatchMq(group, clientId, lockMqs, 3000);
        }
        catch (Exception e)
        {
            Console.WriteLine("    (LockBatchMq 抛异常: " + e.Message + ")");
        }

        double lockMs = sw.Elapsed.TotalMilliseconds;
        Check("S5e 只剩从节点时一台都锁不上（旧口径会退到从节点上锁）",
            locks.Count == 0 && lockMs < LocalBudgetMs,
            lockMs.ToString("F0", CultureInfo.InvariantCulture) + "ms locked=" + FmtQueues(locks));

        // 对照腿：同一份报文直接点名从节点 —— 从节点**本来**就会把锁发出来
        JsonValue lockBody = JsonValue.MakeObject();
        lockBody.Set("consumerGroup", JsonValue.MakeString(group));
        lockBody.Set("clientId", JsonValue.MakeString(clientId));
        JsonValue mqArr = JsonValue.MakeArray();
        JsonValue mqObj = JsonValue.MakeObject();
        mqObj.Set("topic", JsonValue.MakeString(topic));
        mqObj.Set("brokerName", JsonValue.MakeString(BrokerName));
        mqObj.Set("queueId", JsonValue.MakeInt(0));
        mqArr.PushArray(mqObj);
        lockBody.Set("mqSet", mqArr);
        byte[] lockPayload = Bytes(lockBody.Dump());
        long slaveOk = -1;
        try
        {
            RemotingCommand resp = fx.Admin.Client().InvokeSyncRaw(fx.Slave,
                RequestCode.LockBatchMq, null, lockPayload, true, 3000);
            string respText = Encoding.UTF8.GetString(resp.Body ?? Array.Empty<byte>());
            if (Json.TryParse(respText, out JsonValue root, out _) && root is not null)
            {
                JsonValue okSet = root.Get("lockOKMQSet");
                if (okSet.IsArray)
                {
                    slaveOk = okSet.Size();
                }
            }
        }
        catch (Exception e)
        {
            Console.WriteLine("    (从节点锁请求抛异常: " + e.Message + ")");
        }

        Check("S5e2 对照：点名从节点时锁发得出来（空集不是 broker 拒绝）", slaveOk == 1,
            "slave lockOKMQSet=" + slaveOk);

        // 对照腿锁上的那把要还回去（从节点的锁管理器不会有人来解）
        try
        {
            fx.Admin.Client().InvokeSyncRaw(fx.Slave, RequestCode.UnlockBatchMq,
                null, lockPayload, true, 3000);
        }
        catch (Exception)
        {
            // 释放失败不影响断言
        }

        sw.Restart();
        try
        {
            fx.Admin.Client().UnlockBatchMq(group, clientId, lockMqs, 3000);
        }
        catch (Exception e)
        {
            Console.WriteLine("    (UnlockBatchMq 抛异常: " + e.Message + ")");
        }

        double unlockMs = sw.Elapsed.TotalMilliseconds;
        Check("S5e3 解锁同样安静跳过（不抛、不发）", unlockMs < LocalBudgetMs,
            unlockMs.ToString("F0", CultureInfo.InvariantCulture) + "ms");

        // ---------- S5f (E) 订阅口径：POP 只认主 ----------
        // Java PullAPIWrapper#popAsync:369-373：只认主 → 刷一次路由 → 仍没有就本端抛。从节点
        // 不接 POP 这族写请求（ack / 延长不可见时间要落在 broker 侧 revive 表上），退过去只会
        // 换一个可重试的 SYSTEM_BUSY(2)，白烧一轮。
        Console.WriteLine();
        Console.WriteLine("S5f (E) POP 拉取：本端报「The broker[" + BrokerName + "] not exist」（不发 wire）");
        string popText;
        bool popFailed = false;
        bool popIsClientError = false;
        sw.Restart();
        try
        {
            PopResult popResult = fx.Admin.Client().PopMessage(group, topic, 0, 1, 30000, 100, 0);
            popText = "Ok(status=" + popResult.Status + ")";
        }
        catch (MQClientException e)
        {
            popFailed = true;
            popIsClientError = true;
            popText = e.Message;
        }
        catch (Exception e)
        {
            popFailed = true;
            popText = e.GetType().Name + ": " + e.Message;
        }

        double popMs = sw.Elapsed.TotalMilliseconds;
        Check("S5f 停窗口内 POP 本端报「The broker[" + BrokerName + "] not exist」（不是从节点回的错）",
            popFailed && popIsClientError
            && popText == "The broker[" + BrokerName + "] not exist"
            && popMs < LocalBudgetMs,
            popMs.ToString("F0", CultureInfo.InvariantCulture) + "ms " + popText);

        // ---------- S5g (F) 位点读取：刷一次路由后放宽到从节点 ----------
        // Java RemoteBrokerOffsetStore#fetchConsumeOffsetFromBroker:237-241：只认主 → 刷一次
        // 路由 → 重查**放宽**（onlyThisBroker=false，位点是 HA 复制来的同一份数据，可以从从
        // 节点读）。冷实例（路由缓存里没有这个 topic）是这条路径最纯的形状：旧口径在此直接报
        // 「No route info of this topic」，连刷新都没有。
        Console.WriteLine();
        Console.WriteLine("S5g (F) 位点读取：冷实例刷一次路由后放宽到从节点");
        var cold = new MQClientInstance("pr_master_cs_cold_" + fx.Stamp,
            new List<string> { fx.Namesrv });
        try
        {
            string coldDetail;
            bool coldOk = false;
            try
            {
                bool found = cold.QueryConsumerOffset(group,
                    new MessageQueue(topic, BrokerName, 0), out long coldOffset, 5000);
                coldOk = true;
                coldDetail = found
                    ? "offset=" + coldOffset.ToString(CultureInfo.InvariantCulture)
                    : "offset=None(QUERY_NOT_FOUND)";
            }
            catch (Exception e)
            {
                coldDetail = e.GetType().Name + ": " + e.Message;
            }

            Check("S5g 冷实例位点读取不报错：刷路由 → 退到从节点由 broker 答复", coldOk, coldDetail);
        }
        finally
        {
            cold.Shutdown();
        }

        // ---------- S6 (C 端到端) 停窗口内消费 ----------
        Console.WriteLine();
        Console.WriteLine("S6 停窗口内新起的 push 消费者：" + Queues + " 条队列 + 从从节点收齐预埋的 " + Queues + " 条");
        var sink = new Sink();
        DefaultMQPushConsumer consumer = fx.BuildConsumer(group, sink);
        try
        {
            consumer.Start();
        }
        catch (Exception e)
        {
            Check("S6 consumer start", false, e.Message);
            return;
        }

        fx.Consumer = consumer;
        try
        {
            List<MessageQueue> subsInWindow = consumer.Client().GetTopicSubscribeInfo(topic);
            Check("S6a 窗口内消费者自己的订阅信息也是 " + Queues + " 条", subsInWindow.Count == Queues,
                "queues=" + FmtQueues(subsInWindow));
        }
        catch (Exception e)
        {
            Check("S6a 窗口内消费者自己的订阅信息也是 " + Queues + " 条", false, e.Message);
        }

        var expected = new List<string>(seeded);
        expected.Sort();
        bool gotAll = WaitUntil(() =>
        {
            List<string> got = sink.Bodies();
            got.Sort();
            return got.SequenceEqual(expected);
        }, 60000);
        Check("S6 停 master 期间从从节点收齐 " + Queues + " 条", gotAll,
            "got=[" + string.Join(", ", sink.Bodies()) + "]");

        // ---------- S7 负控（先把 master 拉回来） ----------
        Console.WriteLine();
        Console.WriteLine("S7 负控：master 拉回后发布队列恢复、发送恢复");
        (bool restarted, string startOut) = BrokerCtl(script, "start");
        if (!restarted)
        {
            Check("S7 master 复位", false, startOut);
        }

        List<MessageQueue> queuesBack = new();
        bool back = WaitUntil(() =>
        {
            try
            {
                TopicPublishInfo info = fx.Admin.Client().GetTopicPublishInfo(topic, false);
                if (info.MsgQueueList.Count == Queues)
                {
                    queuesBack = info.MsgQueueList;
                    return true;
                }
            }
            catch (Exception)
            {
                // 还没恢复：继续等
            }

            return false;
        }, 60000);
        Check("S7 发布队列恢复 " + Queues, back, "queues=" + FmtQueues(queuesBack));

        // 两条失败发送都没在 broker 上留下消息：每条预埋队列的 maxOffset 仍是 1
        var after = new List<(int QueueId, long Offset)>();
        foreach (MessageQueue mq in back ? queuesBack : queues)
        {
            try
            {
                after.Add((mq.QueueId, fx.MaxOffset(mq)));
            }
            catch (Exception e)
            {
                Console.WriteLine("    (maxOffset 查询失败: " + e.Message + ")");
            }
        }

        Check("S7b 两条失败发送没留下消息（maxOffset 仍是 1）",
            after.Count > 0 && after.All(r => r.Offset == 1), "maxOffset=" + FmtOffsets(after));

        string backText;
        bool backOk = false;
        try
        {
            SendResult r = fx.Producer.Send(new Message(topic, Bytes("pr-master-back")), 20000);
            backOk = r.SendStatus == SendStatus.SendOk;
            backText = "status=" + r.SendStatus + " msgId=" + r.MsgId;
        }
        catch (Exception e)
        {
            backText = e.GetType().Name + ": " + e.Message;
        }

        Check("S7c 发送恢复 SEND_OK", backOk, backText);
    }

    public static int Run(string[] args)
    {
        string namesrv = args.Length > 0 ? args[0] : "127.0.0.1:9876";
        string master = args.Length > 1 ? args[1] : "127.0.0.1:10911";
        string slave = args.Length > 2 ? args[2] : "127.0.0.1:10931";
        string script = ScriptPath();

        Console.WriteLine("broker_ctl=" + script + " namesrv=" + namesrv
                          + " master=" + master + " slave=" + slave);
        (bool up, string upOut) = BrokerCtl(script, "status");
        if (!up)
        {
            Console.WriteLine("master 没在跑：先按本地集群 runbook 起 namesrv + master + slave（" + upOut + "）");
            Check("前置 master UP", false, upOut);
            return Report();
        }

        var fx = new Fixture();
        try
        {
            fx.Start(namesrv, master, slave);
            Scenario(fx, script);
        }
        catch (Exception e)
        {
            Check("联调过程中出现未预期异常", false, e.ToString());
        }
        finally
        {
            fx.Cleanup(script);
        }

        return Report();
    }

    private static int Report()
    {
        Console.WriteLine();
        Console.WriteLine("############ PASS=" + _pass + " FAIL=" + _fail + " ############");
        return _fail == 0 ? 0 : 1;
    }
}
