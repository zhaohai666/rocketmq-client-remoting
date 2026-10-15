// Offline unit tests for the **pull cursor** of the lightweight pull consumer (#105) --
// a fake namesrv + fake broker over real sockets, never touching a real cluster.
//
//
// Why it must be pinned offline: the old implementation only advanced the cursor with
// `msgs[^1].QueueOffset + 1` on FOUND, so when the broker replied NO_MATCHED_MSG (the
// whole range scanned in this round did not match) the cursor did not move -- the next
// round rescans the same range from the same offset and spins forever; the corrected
// value of OFFSET_ILLEGAL was never picked up either, so going out of bounds never
// self-heals. In a real-machine window both show up as "the consumer is alive but never
// receives a message", which is very hard to attribute.
//
// The criteria come from **the traffic on the wire**: the fake broker replies from a
// script and the case looks through MockCluster's evidence for "does the next
// PULL_MESSAGE start from the new offset" -- the cursor really went on the wire, it is
// not just a number changed in an in-memory table.
using System.Linq;
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class LitePullCursorTests
{
    private const string Topic = "LiteCursorTopic";
    private const string GroupPrefix = "GID_LiteCursor";

    private static MessageQueue Queue0() => new(Topic, MockCluster.BrokerName(0), 0);

    private static int Pulls(MockCluster cluster) =>
        cluster.CountRequests(RequestCode.PullMessage) +
        cluster.CountRequests(RequestCode.LitePullMessage);

    private static int PullsFrom(MockCluster cluster, long offset)
    {
        string s = offset.ToString(System.Globalization.CultureInfo.InvariantCulture);
        return cluster.CountRequestsWith(RequestCode.PullMessage, "queueOffset", s) +
               cluster.CountRequestsWith(RequestCode.LitePullMessage, "queueOffset", s);
    }

    /// <summary>
    /// A message body in the 17-field storage format (the same encoding path as in
    /// CodecTests). Hung off a SUCCESS response, used to verify "an in-flight response
    /// colliding with a seek must not enter the buffer".
    /// </summary>
    private static byte[] BodyWithOneMessage(long queueOffset)
    {
        var m = new MessageExt
        {
            Topic = Topic,
            BrokerName = MockCluster.BrokerName(0),
            QueueId = 0,
            QueueOffset = queueOffset,
            SysFlag = 0,
            BornTimestamp = 1700000000000L,
            StoreTimestamp = 1700000000001L,
            BornHost = "127.0.0.1",
            BornHostPort = 10911,
            StoreHost = "127.0.0.1",
            StoreHostPort = 10911,
            CommitLogOffset = 4096,
            Body = Encoding.UTF8.GetBytes("late"),
        };
        return MessageDecoder.EncodeMessageExt(m, false);
    }

    private static Dictionary<string, string> PullResp(long next, long min = 0, long max = 9) => new()
    {
        ["nextBeginOffset"] = next.ToString(System.Globalization.CultureInfo.InvariantCulture),
        ["minOffset"] = min.ToString(System.Globalization.CultureInfo.InvariantCulture),
        ["maxOffset"] = max.ToString(System.Globalization.CultureInfo.InvariantCulture),
    };

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

    /// <summary>
    /// A lite-pull pointing at the fake endpoint (assign + FIRST_OFFSET: the start is the
    /// literal 0, so the first PULL_MESSAGE definitely lands on queueOffset=0).
    /// </summary>
    private static DefaultLitePullConsumer StartedLite(string group, MockCluster cluster)
    {
        var c = new DefaultLitePullConsumer(group);
        c.SetNamesrvAddr(cluster.NamesrvAddr);
        c.SetConsumeFromWhere(ConsumeFromWhere.ConsumeFromFirstOffset);
        c.Assign(new[] { Queue0() });
        c.Start();
        return c;
    }

    // ---------------------------------------------------------------- 1. NO_MATCHED_MSG

    [Fact]
    public void CursorFollowsNextBeginOffsetOnNoMatchedMsg()
    {
        using var cluster = MockCluster.Start(1);
        cluster.ScriptPull(ResponseCode.PullRetryImmediately, PullResp(next: 5));
        DefaultLitePullConsumer c = StartedLite(GroupPrefix + "NoMatch", cluster);
        try
        {
            MessageQueue mq = Queue0();
            Assert.True(WaitFor(() => c.PullCursorOf(mq) == 5),
                "the pull cursor follows nextBeginOffset=5 after NO_MATCHED_MSG");
            // Not just a number changed in an in-memory table: the next PULL_MESSAGE really starts at 5
            Assert.True(WaitFor(() => PullsFrom(cluster, 5) > 0),
                "the next PULL_MESSAGE starts at 5 (the cursor really went on the wire)");
            Assert.Equal(0, cluster.CountRequests(RequestCode.GetMinOffset));
            Assert.Empty(c.Poll(50));
        }
        finally
        {
            c.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 2. OFFSET_ILLEGAL

    /// <summary>
    /// The nextBeginOffset of OFFSET_ILLEGAL is the broker's corrected value for an
    /// out-of-bounds offset: only by following it does "out of bounds self-heals" hold;
    /// stopping at the old offset would receive the same correction every round and spin
    /// in place.
    /// </summary>
    [Fact]
    public void CursorAdoptsTheBrokersOffsetCorrection()
    {
        using var cluster = MockCluster.Start(1);
        cluster.ScriptPull(ResponseCode.PullOffsetMoved, PullResp(next: 42, min: 40));
        DefaultLitePullConsumer c = StartedLite(GroupPrefix + "Illegal", cluster);
        try
        {
            MessageQueue mq = Queue0();
            Assert.True(WaitFor(() => c.PullCursorOf(mq) == 42),
                "the pull cursor adopts the broker's correction after OFFSET_ILLEGAL");
            Assert.True(WaitFor(() => PullsFrom(cluster, 42) > 0),
                "the next PULL_MESSAGE starts at 42");
        }
        finally
        {
            c.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 3. An in-flight seek

    [Fact]
    public void InFlightSeekWinsOverThePullResult()
    {
        // The disposal order is the reverse of the declaration: cluster goes first (it has to join the connection threads still stuck at the gate), the gate after.
        using var gate = new ManualResetEventSlim(false);
        using var cluster = MockCluster.Start(1);
        cluster.ScriptPull(ResponseCode.Success, PullResp(next: 7),
            body: BodyWithOneMessage(queueOffset: 2), gate: gate);
        DefaultLitePullConsumer c = StartedLite(GroupPrefix + "SeekRace", cluster);
        try
        {
            Assert.True(WaitFor(() => Pulls(cluster) >= 1), "the first pull reached the broker");
            MessageQueue mq = Queue0();
            c.Seek(mq, 99);
            gate.Set();

            Assert.True(WaitFor(() => PullsFrom(cluster, 99) > 0), "after the seek the next PULL_MESSAGE starts at 99");
            Assert.Equal(99, c.PullCursorOf(mq));
            Assert.Empty(c.Poll(50));
        }
        finally
        {
            gate.Set();
            c.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 4. The wire contract (#107)

    [Fact]
    public void LitePullCarriesCode361AndTheLiteBit()
    {
        using var cluster = MockCluster.Start(1);
        DefaultLitePullConsumer c = StartedLite(GroupPrefix + "Code361", cluster);
        DefaultMQPullConsumer? classic = null;
        try
        {
            MessageQueue mq = Queue0();
            Assert.True(WaitFor(() => cluster.CountRequests(RequestCode.LitePullMessage) >= 1),
                "a lite consumer pulls with LITE_PULL_MESSAGE(361)");
            Assert.Equal(0, cluster.CountRequests(RequestCode.PullMessage));

            int expectedFlag = PullSysFlag.BuildSysFlag(commitOffset: false, suspend: false,
                subscription: true, classFilter: false, litePull: true);
            foreach (WireRecord rec in cluster.Records().Where(r => r.Code == RequestCode.LitePullMessage))
            {
                int flag = int.Parse(rec.Ext["sysFlag"],
                    System.Globalization.CultureInfo.InvariantCulture);
                Assert.True(PullSysFlag.HasLitePullFlag(flag), "the lite bit must be set");
                Assert.False(PullSysFlag.HasSuspendFlag(flag), "lite is a short poll, the suspend bit is 0");
                Assert.Equal(expectedFlag, flag);
                Assert.Equal("*", rec.Ext["subscription"]);
            }

            // Contrast: a classic pull consumer (the 4-arg version at DefaultMQPullConsumerImpl.pullSyncImpl:248)
            classic = new DefaultMQPullConsumer(GroupPrefix + "Classic");
            classic.SetNamesrvAddr(cluster.NamesrvAddr);
            classic.Start();
            classic.Pull(mq, "*", 0, 32, 3000);
            Assert.True(WaitFor(() => cluster.CountRequests(RequestCode.PullMessage) >= 1),
                "a classic pull uses PULL_MESSAGE(11)");
            WireRecord classicRec = cluster.Records().Last(r => r.Code == RequestCode.PullMessage);
            int classicFlag = int.Parse(classicRec.Ext["sysFlag"],
                System.Globalization.CultureInfo.InvariantCulture);
            Assert.False(PullSysFlag.HasLitePullFlag(classicFlag), "a classic pull carries no lite bit");
            Assert.Equal(PullSysFlag.BuildSysFlag(false, false, true, false), classicFlag);
        }
        finally
        {
            c.Shutdown();
            classic?.Shutdown();
        }
    }
}
