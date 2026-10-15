// Everything goes through the MockCluster fake server (real sockets, scripted
// responses) and never touches a real cluster; queue expansion uses BROADCASTING
// (assign everything, no dependency on a code-38 consumer list response) or feeds
// CLUSTERING precisely via SetReplyBody.
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

    /// <summary>The response body of GET_CONSUMER_LIST_BY_GROUP(38) (same shape as in
    /// PullBalanceViewTests).</summary>
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

    /// <summary>A service instance pointing at the fake cluster: BROADCASTING by default
    /// (all queues, no need to feed a consumer list) plus a 200ms scan interval.</summary>
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

    // ---------------------------------------------------------------- 1. Configuration defaults and guards (offline)

    [Fact]
    public void Defaults_MatchGoldenConfig()
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

        // The delegate overload: wrapped into an IPullTaskCallback in the table and the topic added to RegisterTopics (the source of the heartbeat subscription set)
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
        // The facade Start's "name server address is not set" must blow up as is (the implementation rolls the executor pool back)
        Assert.ThrowsAny<MQClientException>(svc.Start);
        Assert.False(svc.IsStarted);
        Assert.Equal(0, svc.TaskCount);

        // Shutdown is idempotent: whether not started or already started it should just wind down quietly
        svc.Shutdown();
        svc.Shutdown();
    }

    // ---------------------------------------------------------------- 2. Task expansion at queue granularity

    [Fact]
    public void Broadcasting_ExpandsOneTaskPerQueue_AndSeesEveryQueue()
    {
        using var cluster = MockCluster.Start(3); // one read queue per broker in the route => 3 queues
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
                "all three queues must be reached by the callback");
            // The key is the balance view's bare-name queue: the topic carries no namespace
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
        // CLUSTERING expansion has to query the consumer list: the code-38 response only feeds itself => it should get everything
        cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
            _ => CidBody(svc.PullConsumer.ClientId));
        svc.Start();
        try
        {
            Assert.True(WaitFor(() => svc.TaskCount == 3), "the sole instance owns all 3 queues");
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
        // One more peer in the list: with AVG assignment this instance only gets part of
        // them -- it must absolutely not "own all of them" (consuming the same messages
        // twice within a group is the iron rule of a pull facade, see the identical
        // assertion in PullBalanceViewTests).
        cluster.SetReplyBody(RequestCode.GetConsumerListByGroup,
            _ => CidBody("peer-cid-9999", svc.PullConsumer.ClientId));
        svc.Start();
        try
        {
            Assert.True(WaitFor(() => svc.TaskCount > 0), "this instance must get at least one");
            Thread.Sleep(600); // give it two more scan rounds, the number must stay stably below 3
            Assert.True(svc.TaskCount < 3, "with a peer present it must not expand all queues, actual=" + svc.TaskCount);
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
        // Before Start not a single callback is registered => it runs with an empty table;
        // registering after Start works too -- tasks for the new topic are expanded on the
        // next scan round (<= RebalanceScanIntervalMillis).
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
            Assert.True(WaitFor(() => svc.TaskCount == 3, 4000), "the next scan round after registering expands 3 queues");
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
        svc.PullConsumer.PollNameServerIntervalMillis = 200; // let the route cache keep up with the scale-down
        svc.Start();
        try
        {
            Assert.True(WaitFor(() => svc.TaskCount == 3), "starting at 3 queues");
            Assert.True(WaitFor(() => rounds.ContainsKey("broker-0:2")), "queue2 went through a round");

            queueNums = 1; // scale down: the balance view only has queue0 left in the scan round
            Assert.True(WaitFor(() => svc.TaskCount == 1, 4000), "the task table shrinks to 1 after scaling down");

            // A shrink only cancels, it does not hard-cut: the rounds already in flight
            // wind up naturally (400ms >> one round of 250ms + 30ms reschedule), and the
            // round counter of a withdrawn queue must stay frozen forever -- the
            // setCancelled semantics of putTask:66-69.
            Thread.Sleep(400);
            int victimNow = rounds.TryGetValue("broker-0:2", out int v) ? v : 0;
            Thread.Sleep(500);
            Assert.Equal(victimNow, rounds["broker-0:2"]);
            // A surviving queue is not dragged down
            Assert.True(WaitFor(() => rounds.TryGetValue("broker-0:0", out int r) && r > 1));
        }
        finally
        {
            svc.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 3. Delay and serial cadence

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
                "the first round with delay=0 arrives immediately");
            // The delay has not expired: the second round must not show up early (300ms <
            // 600ms, so even with 300ms of slack it must still not be visible)
            Thread.Sleep(delay / 2);
            lock (gate)
            {
                Assert.Single(stamps);
            }

            Assert.True(WaitFor(() => { lock (gate) return stamps.Count >= 2; }, 4000),
                "the second round comes in after the delay expires");
            long t0; long t1;
            lock (gate)
            {
                t0 = stamps[0]; t1 = stamps[1];
            }

            // Task.Delay's tick precision is on the 15.6ms granularity, so 100ms of slack is allowed
            Assert.True(t1 - t0 >= delay - 100,
                $"the second round is not earlier than the first +{delay}ms, actual interval={t1 - t0}ms");
        }
        finally
        {
            svc.Shutdown();
        }
    }

    [Fact]
    public void SameQueue_NeverRunsConcurrently()
    {
        using var cluster = MockCluster.Start(2); // two queues: across queues it may run in parallel, within a queue it must be serial
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
            ctx.PullNextDelayTimeMillis = 5; // fill up the cadence to create the "should parallelise" pressure
        });
        svc.Start();
        try
        {
            Assert.True(WaitFor(() => rounds.Count == 2
                                   && rounds.Values.All(v => v >= 3), 8000),
                "both queues went through several rounds, rounds=[" + string.Join(",", rounds.Select(kv => kv.Key + "=" + kv.Value)) + "]");
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

            ctx.PullNextDelayTimeMillis = 0; // wanting to pull again immediately
            if (n == 1)
            {
                throw new InvalidOperationException("boom");
            }
        });
        svc.Start();
        try
        {
            Assert.True(WaitFor(() => { lock (gate) return stamps.Count >= 2; }, 5000),
                "an exception from the callback must not break through scheduling (it still reschedules after catch Throwable)");
            long t0; long t1;
            lock (gate)
            {
                t0 = stamps[0]; t1 = stamps[1];
            }

            Assert.True(t1 - t0 >= 900, $"after the exception the next round's delay is penalised to 1s, actual interval={t1 - t0}ms");
            Assert.True(t1 - t0 <= 3000, "the penalty is 1s, not never coming back again");
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
        // The first round's broker corrects the cursor to 5; after that the default response echoes the request offset as is
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
            // It really went through the facade and reached the broker: the second round's request offset must be nextBeginOffset=5 from the previous round's response
            Assert.True(WaitFor(() =>
                cluster.CountRequestsWith(RequestCode.PullMessage, "queueOffset", "5") > 0, 6000),
                "the second PULL_MESSAGE starts at the corrected offset 5");
            Assert.Equal(0, cluster.CountRequests(RequestCode.LitePullMessage)); // classic code 11
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
        svc.RebalanceScanIntervalMillis = 100000; // the periodic scan must not interfere, rely on manual PutTask only
        svc.Start();
        try
        {
            var mq = new MessageQueue("GhostTopic", MockCluster.BrokerName(0), 0);
            svc.PutTask("GhostTopic", new[] { mq });
            Assert.Equal(1, svc.TaskCount);
            Thread.Sleep(800); // the first round with delay 0 should long have finished (missing callback => no reschedule)
            Assert.Equal(1, svc.TaskCount);
            // It is in the table: but no PULL_MESSAGE for the GhostTopic went on the wire during those 800ms
            Assert.Equal(0, cluster.Records().Count(r =>
                r.Code == RequestCode.PullMessage
                && r.Ext.TryGetValue("topic", out string? t) && t == "GhostTopic"));
        }
        finally
        {
            svc.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 4. Shutdown draining

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
            gate.Wait(TimeSpan.FromSeconds(5)); // simulating "a round in flight"
            Interlocked.Increment(ref completed);
            ctx.PullNextDelayTimeMillis = 30;
        });
        svc.Start();
        Assert.True(WaitFor(() => Volatile.Read(ref entered) == 1), "the first round got in");

        svc.Shutdown();
        Assert.False(svc.IsStarted);
        Assert.Equal(0, svc.TaskCount); // the task table is emptied
        Assert.Equal(0, Volatile.Read(ref completed)); // the round in flight is still running, not counted ahead of time

        gate.Set(); // let it finish in flight
        Assert.True(WaitFor(() => Volatile.Read(ref completed) == 1), "the in-flight round winds up naturally");
        // The Cancelled/_stopped check in RunTask stops it at that point: no second round is rescheduled
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
            ctx.PullNextDelayTimeMillis = 600; // the second round is still inside the delay
        });
        svc.Start();
        Assert.True(WaitFor(() => Volatile.Read(ref rounds) >= 1));
        svc.Shutdown(); // Disarm withdraws the round waiting at its due time
        Assert.Equal(1, Volatile.Read(ref rounds));

        Thread.Sleep(900); // past the original due time (600ms)
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
        svc.Dispose(); // idempotent
    }
}
