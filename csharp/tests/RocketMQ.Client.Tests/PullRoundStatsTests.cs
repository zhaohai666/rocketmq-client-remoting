// Regression coverage for the PULL-path consume statistics contract, driven through
// the real per-queue pull loop against the in-process fake cluster (no live broker).
//
// The contract the two legs of the loop must jointly honour:
//   ① PULL_RT is booked by a FOUND round only -- and a FOUND round books it even when
//      the answer carries no rows or the pre-delivery filter empties the list. A
//      NO_NEW_MSG / NO_MATCHED_MSG answer and a round that unwinds into a catch book
//      nothing at all.
//   ② PULL_TPS is fed the list AFTER FilterMessagesForDelivery rewrote it, and only
//      when that list is non-empty. A row removed by the second tag pass or by a
//      FilterMessageHook was never delivered, so it must not move the topic's TPS.
// Each case below is built so that reverting one leg turns it red; the comments name
// the reverted reading each case rules out.
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class PullRoundStatsTests
{
    private const string Topic = "PullRoundStatsTopic";
    private const string Group = "GID_PullRoundStats";
    private const string Broker = "broker-0";

    /// <summary>The subscription expression: rows tagged anything else are removed
    /// by the client-side second tag pass before they are ever delivered.</summary>
    private const string KeepTag = "kept";
    private const string DropTag = "dropped";

    private static string StatsKey => Topic + "@" + Group;

    /// <summary>A one-broker fake cluster whose route gives queues ONLY to
    /// <see cref="Topic"/>: the auto-subscribed %RETRY% topic gets none, so exactly
    /// one pull loop ever hits this broker and the scripted answers cannot be stolen
    /// by a sibling queue (which would add rounds this file counts exactly).</summary>
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

    /// <summary>One row in the storage wire format, carrying the tag the client-side
    /// second pass re-checks by string (the fake cluster returns the frames raw, so
    /// this pass is the only gate here).</summary>
    private static byte[] Row(long queueOffset, string tag)
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
        m.Tags = tag;
        return MessageDecoder.EncodeMessageExt(m, false);
    }

    private static byte[] Rows(params byte[][] frames)
    {
        using var ms = new MemoryStream();
        foreach (byte[] f in frames)
        {
            ms.Write(f, 0, f.Length);
        }

        return ms.ToArray();
    }

    /// <summary>Pull response cursors: the loop parks the offset where the answer
    /// points, so every later round asks for a known offset.</summary>
    private static Dictionary<string, string> PullResp(long next) => new()
    {
        ["nextBeginOffset"] = next.ToString(),
        ["minOffset"] = "0",
        ["maxOffset"] = next.ToString(),
    };

    /// <summary>Records every delivered row and succeeds immediately, so the consume
    /// side never holds a pull thread.</summary>
    private sealed class RecordingListener : IMessageListenerConcurrently
    {
        private readonly List<MessageExt> _seen = new();
        private readonly object _gate = new();

        public bool Orderly() => false;

        public List<MessageExt> Seen()
        {
            lock (_gate)
            {
                return new List<MessageExt>(_seen);
            }
        }

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs,
            ConsumeConcurrentlyContext context)
        {
            lock (_gate)
            {
                _seen.AddRange(msgs);
            }

            return ConsumeConcurrentlyStatus.ConsumeSuccess;
        }
    }

    private static DefaultMQPushConsumer NewConsumer(MockCluster cluster, string expression,
        IMessageListenerConcurrently listener, string nameTag)
    {
        var c = new DefaultMQPushConsumer(Group)
        {
            ConsumeFromWhere = ConsumeFromWhere.ConsumeFromFirstOffset,
        };
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.InstanceName = "pull_stats_" + nameTag + "_" + Guid.NewGuid().ToString("N")[..8];
        c.Subscribe(Topic, expression);
        c.SetMessageListener(listener);
        // The clustering allocation reads the group's consumer list from the broker:
        // answer with this instance only, so it owns the queue.
        cluster.SetReplyBody(RequestCode.GetConsumerListByGroup, _ => CidBody(new[] { c.ClientId }));
        return c;
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

    private static bool WaitFor(Func<bool> cond, int millis = 15000)
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

    /// <summary>An untouched key reads as (0, 0): the item being absent IS the verdict
    /// "nothing was booked", so the cases can assert on it uniformly.</summary>
    private static (long value, long times) Booked(StatsItemSet set, string key)
    {
        StatsItem? item = set.Find(key);
        return item is null ? (0L, 0L) : item.Snapshot();
    }

    private static (long value, long times) Rt(DefaultMQPushConsumer c) =>
        Booked(c.Client().ConsumerStats.TopicAndGroupPullRT, StatsKey);

    private static (long value, long times) Tps(DefaultMQPushConsumer c) =>
        Booked(c.Client().ConsumerStats.TopicAndGroupPullTPS, StatsKey);

    // ---------------------------------------------------------------- leg A

    /// <summary>NO_NEW_MSG, NO_MATCHED_MSG and a failed (broker-error) round book
    /// NOTHING: no RT, no TPS. Control: the pre-contract reading called IncPullRT on
    /// every round that returned an answer, so the two non-FOUND rounds land 2 here
    /// and this case goes red.</summary>
    [Fact]
    public void PullLoop_NonFoundAndFailedRounds_BookNeitherRtNorTps()
    {
        using MockCluster cluster = StartCluster();
        cluster.ScriptPull(ResponseCode.PullNotFound, PullResp(0));
        cluster.ScriptPull(ResponseCode.PullRetryImmediately, PullResp(0));
        // Not a status at all: PullMessage throws, the loop's catch runs, the round
        // never reaches the stats point.
        cluster.ScriptPull(ResponseCode.SystemBusy, PullResp(0));

        var listener = new RecordingListener();
        DefaultMQPushConsumer c = NewConsumer(cluster, "*", listener, "nonfound");
        c.Start();
        try
        {
            // The fourth request only goes out once the THIRD round ran to its end,
            // so every scripted answer above has passed the stats point (the failed
            // round backs off before it does).
            Assert.True(WaitFor(() => cluster.CountRequests(RequestCode.PullMessage) >= 4),
                "the pull loop must keep visiting the broker");
            Assert.True(WaitFor(() => cluster.CountRequests(RequestCode.PullMessage) >= 6),
                "and keep visiting it, so no late booking is missed");

            Assert.Equal((0L, 0L), Rt(c));
            Assert.Equal((0L, 0L), Tps(c));
            Assert.Empty(listener.Seen());
            Assert.Equal(0, c.FilteredMessageCount());
        }
        finally
        {
            QuickShutdown(c);
        }
    }

    /// <summary>A FOUND answer that carries no rows at all still books its RT -- the
    /// round did return messages-wise empty, and its trip time is this client's own
    /// measurement -- and books no TPS. Control: narrowing the gate to "FOUND with
    /// rows" drops the RT booking here and goes red.</summary>
    [Fact]
    public void PullLoop_FoundRoundWithNoRows_BooksRtButNoTps()
    {
        using MockCluster cluster = StartCluster();
        cluster.ScriptPull(ResponseCode.Success, PullResp(0));

        var listener = new RecordingListener();
        DefaultMQPushConsumer c = NewConsumer(cluster, "*", listener, "foundempty");
        c.Start();
        try
        {
            // Round 2 arriving proves round 1 walked past both stats points.
            Assert.True(WaitFor(() => cluster.CountRequests(RequestCode.PullMessage) >= 2),
                "the loop must go on to the next round");

            (long rtValue, long rtTimes) = Rt(c);
            Assert.Equal(1, rtTimes);
            Assert.True(rtValue >= 0, "RT is this round's own elapsed milliseconds");
            Assert.Equal((0L, 0L), Tps(c));
            Assert.Empty(listener.Seen());
        }
        finally
        {
            QuickShutdown(c);
        }
    }

    // ---------------------------------------------------------------- leg B

    /// <summary>A FOUND round whose rows are ALL removed by the pre-delivery filter
    /// books RT (it did come back FOUND) and NO TPS: nothing was delivered, so the
    /// topic's throughput did not move. Control: feeding TPS the raw pre-filter count
    /// books 2 here and goes red.</summary>
    [Fact]
    public void PullLoop_FoundRoundFullyFilteredOut_BooksRtButNoTps()
    {
        using MockCluster cluster = StartCluster();
        cluster.ScriptPull(ResponseCode.Success, PullResp(2),
            Rows(Row(0, DropTag), Row(1, DropTag)));

        var listener = new RecordingListener();
        DefaultMQPushConsumer c = NewConsumer(cluster, KeepTag, listener, "fullyfiltered");
        c.Start();
        try
        {
            // The filter runs inside the round, a few statements before the TPS gate:
            // both rows counted as dropped proves the gate has been reached, and the
            // next round proves it ran to its end.
            Assert.True(WaitFor(() => c.FilteredMessageCount() >= 2),
                "both rows must be dropped by the second tag pass");
            Assert.True(WaitFor(() => cluster.CountRequests(RequestCode.PullMessage) >= 2),
                "the loop must go on to the next round");

            Assert.Equal(1, Rt(c).times);
            Assert.Equal((0L, 0L), Tps(c));
            Assert.Empty(listener.Seen());
            Assert.Equal(2, c.FilteredMessageCount());
        }
        finally
        {
            QuickShutdown(c);
        }
    }

    // ---------------------------------------------------------------- leg C

    /// <summary>A FOUND round that keeps 2 of 3 rows books TPS == 2, not the raw 3:
    /// the row the tag pass removed was never delivered. Control: an implementation
    /// that books TPS from the raw response reads 3 here and goes red; one that
    /// books the round's RT after the filter still has to answer 1, which the FOUND
    /// legs above already pin.</summary>
    [Fact]
    public void PullLoop_FoundRoundPartiallyFiltered_TpsCountsKeptRowsOnly()
    {
        using MockCluster cluster = StartCluster();
        cluster.ScriptPull(ResponseCode.Success, PullResp(3),
            Rows(Row(0, KeepTag), Row(1, DropTag), Row(2, KeepTag)));

        var listener = new RecordingListener();
        DefaultMQPushConsumer c = NewConsumer(cluster, KeepTag, listener, "partially");
        c.Start();
        try
        {
            Assert.True(WaitFor(() => listener.Seen().Count >= 2),
                "the two matching rows must reach the listener");
            Assert.True(WaitFor(() => c.FilteredMessageCount() >= 1),
                "the non-matching row must be dropped before delivery");
            Assert.True(WaitFor(() => cluster.CountRequests(RequestCode.PullMessage) >= 2),
                "the loop must go on to the next round");

            (long tpsValue, long tpsTimes) = Tps(c);
            Assert.Equal(1, tpsTimes);
            Assert.Equal(2, tpsValue);
            Assert.Equal(1, Rt(c).times);
            Assert.Equal(1, c.FilteredMessageCount());
            Assert.All(listener.Seen(), m => Assert.Equal(KeepTag, m.Tags));
        }
        finally
        {
            QuickShutdown(c);
        }
    }
}
