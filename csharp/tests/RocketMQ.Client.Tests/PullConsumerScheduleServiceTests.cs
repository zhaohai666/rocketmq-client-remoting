// 全程走 MockCluster 假服务端（真 socket、脚本化应答），不碰真集群；队列展开用
// BROADCASTING（全量分配，不依赖 38 号消费者列表应答）或 SetReplyBody 精确喂 CLUSTERING。
using System.Collections.Concurrent;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class PullConsumerScheduleServiceTests
{
    private const string Topic = "SchedTopic";

    private static bool WaitFor(Func<bool> cond, int timeoutMs = 5000)
    {
        var deadline = Environment.TickCount64 + timeoutMs;
        while (Environment.TickCount64 < deadline)
        {
            if (cond()) return true;
            Thread.Sleep(20);
        }

        return cond();
    }

    private static string Key(MessageQueue mq) =>
        mq.BrokerName + ":" + mq.QueueId.ToString(System.Globalization.CultureInfo.InvariantCulture);

    /// <summary>GET_CONSUMER_LIST_BY_GROUP(38) 的应答体（与 PullBalanceViewTests 同款）。</summary>
    private static byte[] CidBody(params string[] cids)
    {
        var sb = new StringBuilder("{\"consumerIdList\":[");
        for (int i = 0; i < cids.Length; ++i)
        {
            if (i > 0)
            {
                sb.Append(',');
            }

            sb.Append('"').Append(cids[i]).Append('"');
        }

        sb.Append("]}");
        return Encoding.UTF8.GetBytes(sb.ToString());
    }

    /// <summary>指向假集群的服务实例：默认 BROADCASTING（全量队列，免喂消费者列表）+ 200ms 扫描周期。</summary>
    private static PullConsumerScheduleService NewService(MockCluster cluster, string instance,
        string group, Action<MessageQueue, PullTaskContext> cb, bool broadcasting = true,
        int scanMs = 200)
    {
        var svc = new PullConsumerScheduleService(group);
        svc.PullConsumer.SetNamesrvAddr(cluster.NamesrvAddr);
        svc.PullConsumer.SetInstanceName(instance);
        svc.RebalanceScanIntervalMillis = scanMs;
        if (broadcasting)
        {
            svc.PullConsumer.SetMessageModel(MessageModel.Broadcasting);
        }

        svc.RegisterPullTaskCallback(Topic, cb);
        return svc;
    }

    // ---------------------------------------------------------------- 1. 配置默认值与守卫（离线）

    [Fact]
    public void Defaults_MatchJava()
    {
        var svc = new PullConsumerScheduleService("GID_sched_defaults");
        Assert.Equal(20, svc.PullThreadNums);
        Assert.Equal(200, PullTaskContext.DefaultNextDelayMillis);
        Assert.Equal(1000, PullTaskContext.FailureNextDelayMillis);
        Assert.Equal(20000, svc.RebalanceScanIntervalMillis);
        Assert.Equal("GID_sched_defaults", svc.ConsumerGroup);
        Assert.False(svc.IsStarted);
        Assert.Empty(svc.CallbackTable);

        svc.PullThreadNums = 0;
        Assert.Equal(1, svc.PullThreadNums);
    }

    [Fact]
    public void RegisterCallback_GuardsAndTableSemantics()
    {
        var svc = new PullConsumerScheduleService("GID_sched_reg");

        Assert.Throws<ArgumentNullException>(
            () => svc.RegisterPullTaskCallback(Topic, (IPullTaskCallback)null!));

        svc.RegisterPullTaskCallback("", (mq, ctx) => { });
        Assert.Empty(svc.CallbackTable);
        Assert.Empty(svc.PullConsumer.RegisterTopics);

        // delegate 重载：包成 IPullTaskCallback 入表 + topic 进 RegisterTopics（心跳订阅集来源）
        svc.RegisterPullTaskCallback(Topic, (mq, ctx) => { });
        Assert.True(svc.CallbackTable.ContainsKey(Topic));
        Assert.Contains(Topic, svc.PullConsumer.RegisterTopics);

        var other = new NeverPullCallback();
        svc.RegisterPullTaskCallback(Topic, other);
        Assert.Single(svc.CallbackTable);
        Assert.Same(other, svc.CallbackTable[Topic]);
    }

    private sealed class NeverPullCallback : IPullTaskCallback
    {
        public void DoPullTask(MessageQueue mq, PullTaskContext context) { }
    }

    [Fact]
    public void SetPullConsumer_NullAndAfterStart_Rejected()
    {
        var svc = new PullConsumerScheduleService("GID_sched_setconsumer");
        Assert.Throws<MQClientException>(() => svc.SetPullConsumer(null!));

        using var cluster = MockCluster.Start(1);
        svc.PullConsumer.SetNamesrvAddr(cluster.NamesrvAddr);
        svc.PullConsumer.SetInstanceName("sched_setconsumer");
        svc.PullConsumer.SetMessageModel(MessageModel.Broadcasting);
        svc.Start();
        try
        {
            Assert.Throws<MQClientException>(
                () => svc.SetPullConsumer(new DefaultMQPullConsumer("GID_other")));
            Assert.True(svc.IsStarted);
        }
        finally
        {
            svc.Shutdown();
        }
    }

    [Fact]
    public void Start_WithoutNamesrv_ThrowsAndNotStarted()
    {
        var svc = new PullConsumerScheduleService("GID_sched_nonamesrv");
        // 门面 Start 的「name server address is not set」必须原样炸出来（实现里执行池回滚）
        Assert.ThrowsAny<MQClientException>(svc.Start);
        Assert.False(svc.IsStarted);
        Assert.Equal(0, svc.TaskCount);

        // 关停幂等：未 Start / 已 Start 都只该安静收工
        svc.Shutdown();
        svc.Shutdown();
    }

    // ---------------------------------------------------------------- 2. queue 维度任务展开

    [Fact]
    public void Broadcasting_ExpandsOneTaskPerQueue_AndSeesEveryQueue()
    {
        using var cluster = MockCluster.Start(3); // 路由里每台一个读队列 → 3 把队列
        var seen = new ConcurrentDictionary<string, int>();
        var svc = NewService(cluster, "sched_expand", "GID_sched_expand", (mq, ctx) =>
        {
            Assert.Equal(Topic, mq.Topic);
            seen.AddOrUpdate(Key(mq), 1, (_, v) => v + 1);
            ctx.PullNextDelayTimeMillis = 50;
        });
        svc.Start();
        try
        {
            Assert.True(WaitFor(() => svc.TaskCount == 3), "TaskCount=3, actual=" + svc.TaskCount);
            Assert.True(WaitFor(() => seen.Count == 3 && seen.Values.All(v => v > 0)),
                "三把队列都被回调点到");
            // 键就是平衡视图的裸名队列（差异 2）：topic 不带命名空间
            Assert.Equal(new[] { "broker-0", "broker-1", "broker-2" },
                seen.Keys.Select(k => k.Split(':')[0]).Distinct().OrderBy(k => k).ToArray());
        }
        finally
        {
            svc.Shutdown();
        }
    }

    [Fact]
    public void Clustering_SoleInstance_TakesAllQueues()
    {
        using var cluster = MockCluster.Start(3);
        var svc = NewService(cluster, "sched_sole", "GID_sched_sole", (mq, ctx) =>
            ctx.PullNextDelayTimeMillis = 50, broadcasting: false);
        // CLUSTERING 展开要查消费者列表：38 号应答只喂自己一个 → 全量应得
        cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
            _ => CidBody(svc.PullConsumer.ClientId));
        svc.Start();
        try
        {
            Assert.True(WaitFor(() => svc.TaskCount == 3), "唯一实例独占 3 把队列");
        }
        finally
        {
            svc.Shutdown();
        }
    }

    [Fact]
    public void Clustering_WithPeer_TakesOnlyOwnShare()
    {
        using var cluster = MockCluster.Start(3);
        var svc = NewService(cluster, "sched_peer", "GID_sched_peer", (mq, ctx) =>
            ctx.PullNextDelayTimeMillis = 50, broadcasting: false);
        // 列表里多一个对端：AVG 分配后本实例只能拿一部分 —— 绝不能「独占全部」
        // （同组互相重复消费是 pull 门面铁律，见 PullBalanceViewTests 同款断言）。
        cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
            _ => CidBody("peer-cid-9999", svc.PullConsumer.ClientId));
        svc.Start();
        try
        {
            Assert.True(WaitFor(() => svc.TaskCount > 0), "本实例至少拿到一份");
            Thread.Sleep(600); // 再给两轮扫描的时间，数字必须稳定地小于 3
            Assert.True(svc.TaskCount < 3, "有对端时不能展开全部队列，actual=" + svc.TaskCount);
        }
        finally
        {
            svc.Shutdown();
        }
    }

    [Fact]
    public void LateRegistration_ExpandsOnNextScan()
    {
        using var cluster = MockCluster.Start(3);
        // Start 前一条回调都没注册 → 空表空跑；Start 之后注册也生效 ——
        // 差异 1 的口径：下一轮扫描（≤ RebalanceScanIntervalMillis）就为新 topic 展开任务。
        var svc = new PullConsumerScheduleService("GID_sched_late");
        svc.PullConsumer.SetNamesrvAddr(cluster.NamesrvAddr);
        svc.PullConsumer.SetInstanceName("sched_late");
        svc.PullConsumer.SetMessageModel(MessageModel.Broadcasting);
        svc.RebalanceScanIntervalMillis = 150;
        svc.Start();
        try
        {
            Assert.Equal(0, svc.TaskCount);
            svc.RegisterPullTaskCallback(Topic, (mq, ctx) => ctx.PullNextDelayTimeMillis = 50);
            Assert.True(WaitFor(() => svc.TaskCount == 3, 4000), "注册后下一轮扫描展开 3 把队列");
        }
        finally
        {
            svc.Shutdown();
        }
    }

    [Fact]
    public void PutTask_ShrinksRemovedQueues_AndStopsTheirRounds()
    {
        using var cluster = MockCluster.Start(1);
        int queueNums = 3;
        cluster.SetRouteBody(_ =>
        {
            var route = new TopicRouteData();
            route.QueueDatas.Add(new QueueData(MockCluster.BrokerName(0), queueNums, queueNums,
                PermName.PermRead | PermName.PermWrite, 0));
            route.BrokerDatas.Add(new BrokerData("MockCluster", MockCluster.BrokerName(0),
                new SortedDictionary<long, string> { { MixAll.MasterId, cluster.BrokerAddrs[0] } }));
            return route.Encode();
        });
        var rounds = new ConcurrentDictionary<string, int>();
        var svc = NewService(cluster, "sched_shrink", "GID_sched_shrink", (mq, ctx) =>
        {
            rounds.AddOrUpdate(Key(mq), 1, (_, v) => v + 1);
            ctx.PullNextDelayTimeMillis = 30;
        });
        svc.PullConsumer.PollNameServerIntervalMillis = 200; // 让路由缓存跟上缩容
        svc.Start();
        try
        {
            Assert.True(WaitFor(() => svc.TaskCount == 3), "起步 3 把队列");
            Assert.True(WaitFor(() => rounds.ContainsKey("broker-0:2")), "queue2 跑过轮");

            queueNums = 1; // 缩容：扫描轮里平衡视图只剩 queue0
            Assert.True(WaitFor(() => svc.TaskCount == 1, 4000), "缩容后任务表收缩到 1");

            // 收缩只 cancel 不硬切：等在途的那几轮自然收工（400ms ≫ 单轮 250ms + 重排 30ms），
            // 之后被撤队列的轮次计数必须永远定格 —— putTask:66-69 的 setCancelled 语义。
            Thread.Sleep(400);
            int victimNow = rounds.TryGetValue("broker-0:2", out int v) ? v : 0;
            Thread.Sleep(500);
            Assert.Equal(victimNow, rounds["broker-0:2"]);
            // 幸存队列不受牵连
            Assert.True(WaitFor(() => rounds.TryGetValue("broker-0:0", out int r) && r > 1));
        }
        finally
        {
            svc.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 3. 延迟与串行节奏

    [Fact]
    public void NextRoundWaitsForPullNextDelay()
    {
        using var cluster = MockCluster.Start(1);
        var stamps = new List<long>();
        var gate = new object();
        const int delay = 600;
        var svc = NewService(cluster, "sched_delay", "GID_sched_delay", (mq, ctx) =>
        {
            lock (gate)
            {
                stamps.Add(Environment.TickCount64);
            }

            ctx.PullNextDelayTimeMillis = delay;
        });
        svc.Start();
        try
        {
            Assert.True(WaitFor(() => { lock (gate) return stamps.Count >= 1; }),
                "第一轮 delay=0 立即来（Java putTask:77 同款）");
            // 延迟未到期：第二轮不许提前出现（300ms < 600ms，留 300ms 余量仍不该看到）
            Thread.Sleep(delay / 2);
            lock (gate)
            {
                Assert.Single(stamps);
            }

            Assert.True(WaitFor(() => { lock (gate) return stamps.Count >= 2; }, 4000),
                "延迟到期后第二轮进来");
            long t0; long t1;
            lock (gate)
            {
                t0 = stamps[0]; t1 = stamps[1];
            }

            // Task.Delay 的点位精度在 15.6ms 档上，允许 100ms 提前量
            Assert.True(t1 - t0 >= delay - 100,
                $"第二轮不早于第一轮 +{delay}ms，实际间隔={t1 - t0}ms");
        }
        finally
        {
            svc.Shutdown();
        }
    }

    [Fact]
    public void SameQueue_NeverRunsConcurrently()
    {
        using var cluster = MockCluster.Start(2); // 两把队列：跨队列可以并行，队列内必须串行
        var inflight = new ConcurrentDictionary<string, int>();
        var maxInflight = new ConcurrentDictionary<string, int>();
        var rounds = new ConcurrentDictionary<string, int>();
        var svc = NewService(cluster, "sched_serial", "GID_sched_serial", (mq, ctx) =>
        {
            string key = Key(mq);
            int now = inflight.AddOrUpdate(key, 1, (_, v) => v + 1);
            maxInflight.AddOrUpdate(key, now, (_, m) => Math.Max(m, now));
            Thread.Sleep(250);
            inflight.AddOrUpdate(key, 0, (_, v) => v - 1);
            rounds.AddOrUpdate(key, 1, (_, v) => v + 1);
            ctx.PullNextDelayTimeMillis = 5; // 排满节奏，制造「该并发」的压力
        });
        svc.Start();
        try
        {
            Assert.True(WaitFor(() => rounds.Count == 2
                                   && rounds.Values.All(v => v >= 3), 8000),
                "两把队列都轮到了多轮，rounds=[" + string.Join(",", rounds.Select(kv => kv.Key + "=" + kv.Value)) + "]");
            foreach ((string key, int max) in maxInflight)
            {
                Assert.Equal(1, max);
            }
        }
        finally
        {
            svc.Shutdown();
        }
    }

    [Fact]
    public void CallbackThrows_PenalizedTo1000ms_AndScheduleSurvives()
    {
        using var cluster = MockCluster.Start(1);
        var stamps = new List<long>();
        var gate = new object();
        int attempt = 0;
        var svc = NewService(cluster, "sched_throw", "GID_sched_throw", (mq, ctx) =>
        {
            int n;
            lock (gate)
            {
                n = ++attempt;
                stamps.Add(Environment.TickCount64);
            }

            ctx.PullNextDelayTimeMillis = 0; // 想立刻续拉
            if (n == 1)
            {
                throw new InvalidOperationException("boom");
            }
        });
        svc.Start();
        try
        {
            Assert.True(WaitFor(() => { lock (gate) return stamps.Count >= 2; }, 5000),
                "回调抛异常不能打穿调度（Java :182-185 catch Throwable 后照样重排）");
            long t0; long t1;
            lock (gate)
            {
                t0 = stamps[0]; t1 = stamps[1];
            }

            Assert.True(t1 - t0 >= 900, $"异常后下一轮延迟罚到 1s，实际间隔={t1 - t0}ms");
            Assert.True(t1 - t0 <= 3000, "罚的是 1s 不是永远不再来");
        }
        finally
        {
            svc.Shutdown();
        }
    }

    [Fact]
    public void CallbackPullsThroughFacade_AndFollowsNextBeginOffset()
    {
        using var cluster = MockCluster.Start(1);
        // 第一轮 broker 把游标纠正到 5；此后默认应答原样回显请求位点
        cluster.ScriptPull(ResponseCode.PullNotFound, new Dictionary<string, string>
        {
            ["nextBeginOffset"] = "5", ["minOffset"] = "0", ["maxOffset"] = "9",
        });
        long offset = 0;
        var statuses = new List<string>();
        var gate = new object();
        var svc = NewService(cluster, "sched_pull", "GID_sched_pull", (mq, ctx) =>
        {
            PullResult r = ctx.PullConsumer.Pull(mq, "*", Interlocked.Read(ref offset), 32, 3000);
            Interlocked.Exchange(ref offset, r.NextBeginOffset);
            lock (gate)
            {
                statuses.Add(r.Status.ToString());
            }

            ctx.PullNextDelayTimeMillis = 20;
        });
        svc.Start();
        try
        {
            // 真走门面拉到了 broker：第二轮的请求位点必须是上一轮应答里的 nextBeginOffset=5
            Assert.True(WaitFor(() =>
                cluster.CountRequestsWith(RequestCode.PullMessage, "queueOffset", "5") > 0, 6000),
                "第二轮 PULL_MESSAGE 从纠正后的位点 5 起");
            Assert.Equal(0, cluster.CountRequests(RequestCode.LitePullMessage)); // 经典码 11
            lock (gate)
            {
                Assert.Equal("NoNewMsg", statuses[0]); // PullNotFound → NO_NEW_MSG
            }
        }
        finally
        {
            svc.Shutdown();
        }
    }

    [Fact]
    public void MissingCallback_WarnsAndDoesNotReschedule()
    {
        using var cluster = MockCluster.Start(1);
        var svc = new PullConsumerScheduleService("GID_sched_nocb");
        svc.PullConsumer.SetNamesrvAddr(cluster.NamesrvAddr);
        svc.PullConsumer.SetInstanceName("sched_nocb");
        svc.RebalanceScanIntervalMillis = 100000; // 周期扫描别来搅局，全靠手动 PutTask
        svc.Start();
        try
        {
            var mq = new MessageQueue("GhostTopic", MockCluster.BrokerName(0), 0);
            svc.PutTask("GhostTopic", new[] { mq });
            Assert.Equal(1, svc.TaskCount);
            Thread.Sleep(800); // delay 0 的首轮早就该跑完（回调缺失 → 不重排）
            Assert.Equal(1, svc.TaskCount);
            // 表里是它：但 800ms 里没有任何 GhostTopic 的 PULL_MESSAGE 上线
            Assert.Equal(0, cluster.Records().Count(r =>
                r.Code == RequestCode.PullMessage
                && r.Ext.TryGetValue("topic", out string? t) && t == "GhostTopic"));
        }
        finally
        {
            svc.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 4. Shutdown 排空

    [Fact]
    public void Shutdown_DrainsInFlightRoundAndNeverReschedules()
    {
        using var cluster = MockCluster.Start(1);
        using var gate = new ManualResetEventSlim(false);
        int entered = 0;
        int completed = 0;
        var svc = NewService(cluster, "sched_drain", "GID_sched_drain", (mq, ctx) =>
        {
            Interlocked.Increment(ref entered);
            gate.Wait(TimeSpan.FromSeconds(5)); // 模拟「在途的一轮回调」
            Interlocked.Increment(ref completed);
            ctx.PullNextDelayTimeMillis = 30;
        });
        svc.Start();
        Assert.True(WaitFor(() => Volatile.Read(ref entered) == 1), "第一轮进来了");

        svc.Shutdown();
        Assert.False(svc.IsStarted);
        Assert.Equal(0, svc.TaskCount); // 任务表清空
        Assert.Equal(0, Volatile.Read(ref completed)); // 在途的那轮还在跑，没被抢先记账

        gate.Set(); // 放它在途跑完
        Assert.True(WaitFor(() => Volatile.Read(ref completed) == 1), "在途轮次自然收工");
        // 收工处 RunTask 的 Cancelled/_stopped 检查把它掐住：不再重排第二轮
        Thread.Sleep(200);
        Assert.Equal(1, Volatile.Read(ref entered));
        Assert.Equal(1, Volatile.Read(ref completed));
    }

    [Fact]
    public void Shutdown_CancelsPendingDelayedRound()
    {
        using var cluster = MockCluster.Start(1);
        int rounds = 0;
        var svc = NewService(cluster, "sched_disarm", "GID_sched_disarm", (mq, ctx) =>
        {
            Interlocked.Increment(ref rounds);
            ctx.PullNextDelayTimeMillis = 600; // 第二轮还压在延迟里
        });
        svc.Start();
        Assert.True(WaitFor(() => Volatile.Read(ref rounds) >= 1));
        svc.Shutdown(); // Disarm 撤掉在等点位的轮次
        Assert.Equal(1, Volatile.Read(ref rounds));

        Thread.Sleep(900); // 越过原本的到期点位（600ms）
        Assert.Equal(1, Volatile.Read(ref rounds));
    }

    [Fact]
    public void Dispose_CallsShutdown()
    {
        using var cluster = MockCluster.Start(1);
        var svc = NewService(cluster, "sched_dispose", "GID_sched_dispose", (mq, ctx) =>
            ctx.PullNextDelayTimeMillis = 50);
        svc.Start();
        Assert.True(WaitFor(() => svc.TaskCount == 1));
        svc.Dispose();
        Assert.False(svc.IsStarted);
        Assert.Equal(0, svc.TaskCount);
        svc.Dispose(); // 幂等
    }
}
