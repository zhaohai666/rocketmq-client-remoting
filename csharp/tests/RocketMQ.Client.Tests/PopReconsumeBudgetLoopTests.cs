// End-to-end coverage for the POP pre-delivery budget drop and its pull-path
// counterpart, against the in-process fake cluster.
//
// The pipeline-level cases in PopConsumerTests pin the pure legs; these two pin the
// wiring: the over-budget row must really be ACKED on the wire BEFORE delivery while
// the rows within budget are delivered un-acked, and the same over-budget row on the
// pull path (which shares the tag/hook filter but has no budget leg and no ack to
// give) must still reach the listener.
using System.Globalization;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class PopReconsumeBudgetLoopTests
{
    private const string Topic = "PopBudgetLoopTopic";
    private const string Group = "GID_PopBudgetLoop";
    private const string Broker = "broker-0";

    /// <summary>A one-broker fake cluster whose route gives queues ONLY to
    /// <see cref="Topic"/>: the auto-subscribed %RETRY% topic gets none, so exactly
    /// one POP loop (or one pull loop) ever hits this broker and the scripted
    /// answers cannot be stolen by a sibling queue.</summary>
    private static MockCluster StartCluster()
    {
        var cluster = MockCluster.Start(1);
        cluster.SetRouteBody(req =>
        {
            string asked = req.ExtFields.TryGetValue("topic", out string? t) ? t : "";
            var route = new TopicRouteData();
            if (asked == Topic)
            {
                route.QueueDatas.Add(new QueueData(Broker, 1, 1,
                    PermName.PermRead | PermName.PermWrite, 0));
            }

            route.BrokerDatas.Add(new BrokerData("MockCluster", Broker,
                new SortedDictionary<long, string> { { MixAll.MasterId, cluster.BrokerAddrs[0] } }));
            return route.Encode();
        });
        return cluster;
    }

    /// <summary>One row in the storage wire format: the reconsume count rides the
    /// binary header field, so it survives the broker round trip verbatim.</summary>
    private static byte[] RowBody(long queueOffset, int reconsumeTimes)
    {
        var m = new MessageExt
        {
            Topic = Topic,
            BrokerName = Broker,
            QueueId = 0,
            QueueOffset = queueOffset,
            SysFlag = 0,
            BornTimestamp = UtilAll.CurrentTimeMillis(),
            StoreTimestamp = UtilAll.CurrentTimeMillis(),
            BornHost = "127.0.0.1",
            BornHostPort = 10911,
            StoreHost = "127.0.0.1",
            StoreHostPort = 10911,
            CommitLogOffset = 4096 + 512 * queueOffset,
            Body = new byte[] { 1, 2, 3 },
        };
        m.ReconsumeTimes = reconsumeTimes;
        return MessageDecoder.EncodeMessageExt(m, false);
    }

    private static byte[] Concat(params byte[][] frames)
    {
        using var ms = new MemoryStream();
        foreach (byte[] f in frames)
        {
            ms.Write(f, 0, f.Length);
        }

        return ms.ToArray();
    }

    /// <summary>Holds every delivered row until the test releases it, so the rows
    /// are provably NOT acked while the wire is inspected (a listener that returned
    /// CONSUME_SUCCESS immediately would ack them itself and blur the evidence).</summary>
    private sealed class HoldingListener : IMessageListenerConcurrently
    {
        private readonly List<MessageExt> _seen = new();
        private readonly object _gate = new();
        private readonly ManualResetEventSlim _release = new(false);

        public bool Orderly() => false;

        public List<MessageExt> Seen()
        {
            lock (_gate)
            {
                return new List<MessageExt>(_seen);
            }
        }

        public void Release() => _release.Set();

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
        {
            lock (_gate)
            {
                _seen.AddRange(msgs);
            }

            _release.Wait(TimeSpan.FromSeconds(15));
            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    /// <summary>Returns CONSUME_SUCCESS immediately, so a kept row is acked by the
    /// listener itself and never held -- the re-schedule wiring tests must not block
    /// the shutdown path on a held consume thread.</summary>
    private sealed class SuccessListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
            => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }

    private static bool WaitFor(Func<bool> cond, int millis = 10000)
    {
        long deadline = Environment.TickCount64 + millis;
        while (Environment.TickCount64 < deadline)
        {
            if (cond()) return true;
            Thread.Sleep(20);
        }

        return cond();
    }

    /// <summary>Shutdown without paying one whole rebalance period: the rebalance
    /// thread blocks in a long wait that the stop flags do not wake.</summary>
    private static void QuickShutdown(DefaultMQPushConsumer c)
    {
        Exception? error = null;
        Thread worker = new(() =>
        {
            try
            {
                c.Shutdown();
            }
            catch (Exception e)
            {
                error = e;
            }
        })
        { IsBackground = true };
        worker.Start();
        while (!worker.Join(50))
        {
            c.WakeupRebalanceForTest();
        }

        if (error is not null)
        {
            throw error;
        }
    }

    private static byte[] CidBody(string[] cids)
    {
        var sb = new StringBuilder("{\"consumerIdList\":[");
        for (int i = 0; i < cids.Length; i++)
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

    /// <summary>Budget 2 against rows at reconsume counts 1, 2 and 3: ONLY the 3 is
    /// dropped, and its ACK is on the wire before delivery while the two delivered
    /// rows sit un-acked in the listener. Control legs: a ">=" reading would ack the
    /// row at 2 as well (three rows gone / offset "1" acked), and a pipeline that
    /// forgot the pre-delivery ack would show ZERO acks while all three rows reach
    /// the listener.</summary>
    [Fact]
    public void PopLoop_AcksTheOverBudgetRowBeforeDelivery_AndDeliversTheRestUnacked()
    {
        using MockCluster cluster = StartCluster();
        int popRounds = 0;
        cluster.SetReplyBody(RequestCode.PopMessage, req =>
        {
            string asked = req.ExtFields.TryGetValue("topic", out string? t) ? t : "";
            // Only the FIRST pop of the business topic answers with rows; every
            // later round comes back empty so no second drop can pollute the wire.
            if (asked != Topic || Interlocked.Increment(ref popRounds) > 1)
            {
                return Array.Empty<byte>();
            }

            return Concat(RowBody(0, 1), RowBody(1, 2), RowBody(2, 3));
        });
        cluster.SetReplyExt(RequestCode.PopMessage, _ => new Dictionary<string, string>
        {
            // A fresh popTime per request keeps the batch inside its invisible
            // window; without it the checkpoint would read as timed out.
            ["popTime"] = UtilAll.CurrentTimeMillis().ToString(CultureInfo.InvariantCulture),
            ["invisibleTime"] = "60000",
            ["reviveQid"] = "0",
        });

        var listener = new HoldingListener();
        var c = new DefaultMQPushConsumer(Group)
        {
            PopMode = true,
            MaxReconsumeTimes = 2,
        };
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.InstanceName = "budget_loop_" + Guid.NewGuid().ToString("N")[..8];
        c.Subscribe(Topic, "*");
        c.SetMessageListener(listener);
        // The clustering allocation reads the group's consumer list from the
        // broker: answer with this instance only, so it owns the queue.
        cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(new[] { c.ClientId }));
        c.Start();
        try
        {
            Assert.True(WaitFor(() => cluster.CountRequests(RequestCode.AckMessage) >= 1),
                "the over-budget row must be acked against the original popped list");
            Assert.True(WaitFor(() =>
                {
                    List<MessageExt> seen = listener.Seen();
                    return seen.Count >= 2;
                }),
                "the rows within budget must reach the listener");

            List<WireRecord> acks = cluster.Records().FindAll(
                r => r.Code == RequestCode.AckMessage);
            WireRecord ack = Assert.Single(acks);
            Assert.Equal(Group, ack.Ext["consumerGroup"]);
            Assert.Equal(Topic, ack.Ext["topic"]);
            // The dropped row is the one at queue offset 2 (reconsume count 3);
            // the two delivered rows (offsets 0 and 1) are still held by the
            // listener, so NOTHING else may be acked at this point.
            Assert.Equal("2", ack.Ext["offset"]);

            List<MessageExt> seen = listener.Seen();
            Assert.Equal(new long[] { 0, 1 }, seen.Select(m => m.QueueOffset).OrderBy(x => x).ToArray());
            Assert.All(seen, m => Assert.True(m.ReconsumeTimes <= 2,
                "a row over the budget must not be delivered, saw reconsume " + m.ReconsumeTimes));

            // The drop is booked on the shared filtered counter.
            Assert.Equal(1, c.FilteredMessageCount());

            // Loop-level stats leg: the round books pull TPS from the KEPT rows
            // only (2 of 3), booked once -- the later Found-empty rounds move RT
            // but never TPS. An implementation that books stats on the raw
            // popped list reads 3 here and goes red.
            string statsKey = Topic + "@" + Group;
            (long tpsValue, long tpsTimes) = c.Client().ConsumerStats
                .TopicAndGroupPullTPS.Find(statsKey)!.Snapshot();
            Assert.Equal(2, tpsValue);
            Assert.Equal(1, tpsTimes);
            Assert.True(c.Client().ConsumerStats.TopicAndGroupPullRT
                .Find(statsKey)!.Snapshot().times >= 1);
        }
        finally
        {
            listener.Release();
            QuickShutdown(c);
        }
    }

    /// <summary>The budget leg is POP-only. The pull path runs the shared
    /// tag/hook filter without it and has no ack to give (its offset advances
    /// anyway), so the SAME over-budget row (reconsume 3 against budget 2) must
    /// still be delivered there. Control: hoisting the drop into the shared
    /// filter swallows the row before the buffer and this wait goes red.</summary>
    [Fact]
    public void PullPath_StillDeliversTheOverBudgetRow()
    {
        using MockCluster cluster = StartCluster();
        cluster.ScriptPull(ResponseCode.Success, new Dictionary<string, string>
        {
            ["nextBeginOffset"] = "1",
            ["minOffset"] = "0",
            ["maxOffset"] = "1",
        }, RowBody(0, 3));

        var listener = new HoldingListener();
        var c = new DefaultMQPushConsumer(Group)
        {
            MaxReconsumeTimes = 2,
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.InstanceName = "budget_pull_" + Guid.NewGuid().ToString("N")[..8];
        c.Subscribe(Topic, "*");
        c.SetMessageListener(listener);
        cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(new[] { c.ClientId }));
        c.Start();
        try
        {
            Assert.True(WaitFor(() =>
                {
                    List<MessageExt> seen = listener.Seen();
                    return seen.Any(m => m.QueueOffset == 0 && m.ReconsumeTimes == 3);
                }),
                "the over-budget row must still be delivered on the pull path");
            Assert.Equal(0, c.FilteredMessageCount());
        }
        finally
        {
            listener.Release();
            QuickShutdown(c);
        }
    }

    /// <summary>Starts a POP-mode consumer that only ever sees the scripted
    /// answer (no rows needed) and returns the arrival times of its pop rounds,
    /// so the re-schedule legs are measured on the wire instead of racing a
    /// timer inside the loop.</summary>
    private static List<long> PopRoundArrivals(MockCluster cluster, DefaultMQPushConsumer c,
        int wantRounds)
    {
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.Subscribe(Topic, "*");
        cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(new[] { c.ClientId }));
        c.Start();
        try
        {
            Assert.True(WaitFor(() => cluster.CountRequests(RequestCode.PopMessage) >= wantRounds),
                "the POP loop must keep visiting the broker");
            return cluster.Records()
                .FindAll(r => r.Code == RequestCode.PopMessage)
                .GetRange(0, wantRounds)
                .Select(r => r.ArrivalMs)
                .ToList();
        }
        finally
        {
            QuickShutdown(c);
        }
    }

    /// <summary>A POLLING_FULL answer parks the queue for the 3000 ms
    /// back-off: the gap between two consecutive pop rounds is that wait, not
    /// the 200 ms hot-round nap. Control: the old "fall through to the next
    /// round" reading answers within a few hundred ms and goes red.</summary>
    [Fact]
    public void PopLoop_PollingFullAnswer_BackOffsThreeSeconds()
    {
        using MockCluster cluster = StartCluster();
        cluster.SetReplyCode(RequestCode.PopMessage, ResponseCode.PollingFull);
        var c = new DefaultMQPushConsumer(Group)
        {
            PopMode = true,
            InstanceName = "budget_pollingfull_" + Guid.NewGuid().ToString("N")[..8],
        };
        c.SetMessageListener(new HoldingListener());

        List<long> arrivals = PopRoundArrivals(cluster, c, 2);
        long gap = arrivals[1] - arrivals[0];
        Assert.InRange(gap, DefaultMQPushConsumer.PopExceptionBackoffMillis - 100,
            DefaultMQPushConsumer.PopExceptionBackoffMillis + 2000);
    }

    /// <summary>POLLING_NOT_FOUND is a normal empty answer: straight to the
    /// next round (only the 200 ms hot-round nap applies). Control: an
    /// implementation that parks every empty status on the 3000 ms back-off
    /// reads a gap over 2900 here and goes red.</summary>
    [Fact]
    public void PopLoop_PollingNotFoundAnswer_GoesStraightToTheNextRound()
    {
        using MockCluster cluster = StartCluster();
        cluster.SetReplyCode(RequestCode.PopMessage, ResponseCode.PollingTimeout);
        var c = new DefaultMQPushConsumer(Group)
        {
            PopMode = true,
            InstanceName = "budget_pollingnf_" + Guid.NewGuid().ToString("N")[..8],
        };
        c.SetMessageListener(new HoldingListener());

        List<long> arrivals = PopRoundArrivals(cluster, c, 2);
        Assert.True(arrivals[1] - arrivals[0] < 750,
            "an empty poll must not park the queue on the exception back-off, gap was "
            + (arrivals[1] - arrivals[0]) + " ms");
    }

    /// <summary>A Found answer WITH kept rows paces the next round by
    /// PullIntervalMillis (800 ms here): the gap between the round that
    /// delivered and the next round is that wait -- clearly above the 200 ms
    /// nap and clearly below the 3000 ms back-off.</summary>
    [Fact]
    public void PopLoop_FoundWithKeptRows_PacesTheNextRoundByPullInterval()
    {
        using MockCluster cluster = StartCluster();
        int popRounds = 0;
        cluster.SetReplyBody(RequestCode.PopMessage, req =>
        {
            string asked = req.ExtFields.TryGetValue("topic", out string? t) ? t : "";
            if (asked != Topic || Interlocked.Increment(ref popRounds) > 1)
            {
                return Array.Empty<byte>();
            }

            return Concat(RowBody(0, 1));
        });
        cluster.SetReplyExt(RequestCode.PopMessage, _ => new Dictionary<string, string>
        {
            ["popTime"] = UtilAll.CurrentTimeMillis().ToString(CultureInfo.InvariantCulture),
            ["invisibleTime"] = "60000",
            ["reviveQid"] = "0",
        });

        var listener = new SuccessListener();
        var c = new DefaultMQPushConsumer(Group)
        {
            PopMode = true,
            PullIntervalMillis = 800,
            InstanceName = "budget_interval_" + Guid.NewGuid().ToString("N")[..8],
        };
        c.SetMessageListener(listener);

        List<long> arrivals = PopRoundArrivals(cluster, c, 2);
        long gap = arrivals[1] - arrivals[0];
        Assert.InRange(gap, 750, 2500);
    }
}
