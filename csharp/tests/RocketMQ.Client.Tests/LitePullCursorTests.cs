// 轻量拉取消费者的**拉取游标**（#105）离线单测 —— 假 namesrv + 假 broker（真 socket），
// 不碰真集群。
//
// 对齐基准（Java 5.5.1）：DefaultLitePullConsumerImpl#PullTaskImpl.run:982-998 —— 一轮
// 拉取**成功返回**之后，无论 FOUND / NO_NEW_MSG / NO_MATCHED_MSG / OFFSET_ILLEGAL，都把
// 拉取游标推进到 pullResult.getNextBeginOffset()（:998 updatePullOffset）；唯一的刹车是
// 「这轮里刚 seek 过」（:808 的 getSeekOffset(mq) == -1 检查）与「队列已被撤走」
// （AssignedMessageQueue.updatePullOffset:82-91 的 processQueue 身份比对），FOUND 分支的
// 入缓冲（:986）挂的是同一只刹车。
//
// 为什么必须离线锁死：旧实现只在 FOUND 时用 `msgs[^1].QueueOffset + 1` 推进游标，于是
// broker 回 NO_MATCHED_MSG（本轮扫过的整段都不匹配）时游标原地不动 —— 下一轮从同一位点
// 把同一段重扫一遍，永远打转；OFFSET_ILLEGAL 的纠正值也吃不到，越界不自愈。真机窗口里
// 两者都表现为「消费者活着但永远收不到消息」，很难归因。
//
// 判据取自**线上报文**：假 broker 的应答按脚本回，用例从 MockCluster 的取证里查
// 「下一笔 PULL_MESSAGE 是不是从新位点起的」—— 游标真的上了线，而不只是内存表里改了个数。
// 与 cpp/tests/test_lite_pull_cursor.cpp、rust/src/client/pull_consumer.rs、
// python/tests/test_lite_pull_consumer.py 的同名用例同题。
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

    /// <summary>PULL_MESSAGE 与 LITE_PULL_MESSAGE 都算（Java PullMessageProcessor 同口径）。</summary>
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
    /// 一篇 17 段存储格式的消息体（与 CodecTests 同一条编码路径）。挂在 SUCCESS 应答上，
    /// 用来验「在途应答撞上 seek 时不许入缓冲」。
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
    /// 指向假端点的 lite-pull（assign + FIRST_OFFSET：起点是字面量 0，第一笔 PULL_MESSAGE
    /// 确定落在 queueOffset=0）。
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

    /// <summary>
    /// Java :982-998：NO_MATCHED_MSG 的 nextBeginOffset 已越过本轮扫过的整段不匹配区间，
    /// 拉取游标必须跟过去 —— 旧实现只在 FOUND 时推游标，会永远卡在 0 重扫同一段。
    /// </summary>
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
                "NO_MATCHED_MSG 后拉取游标跟到 nextBeginOffset=5");
            // 不是内存表里改了个数：下一笔 PULL_MESSAGE 真的从 5 起
            Assert.True(WaitFor(() => PullsFrom(cluster, 5) > 0),
                "下一笔 PULL_MESSAGE 从 5 开始（游标真的上了线）");
            // FIRST_OFFSET 的起点是字面量 0（Java RebalanceLitePullImpl:114-124），不该发 minOffset 查询
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
    /// OFFSET_ILLEGAL 的 nextBeginOffset 是 broker 对越界位点的纠正值：跟过去才算「越界自愈」，
    /// 停在旧位点会每轮收到同一个纠正、原地打转。
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
                "OFFSET_ILLEGAL 后拉取游标采纳 broker 纠正");
            Assert.True(WaitFor(() => PullsFrom(cluster, 42) > 0),
                "下一笔 PULL_MESSAGE 从 42 开始");
        }
        finally
        {
            c.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 3. 在途 seek

    /// <summary>
    /// 唯一一只刹车（Java :808 的 seekOffset == -1 + :979 的 isDropped）：在途应答回来时，
    /// 这轮里刚 seek 过的位点不许被盖掉，也不许把应答里的消息塞进缓冲（seek 的语义就是
    /// 「游标钉在这里、旧位点的消息全丢」）。发车闸把在途窗口拉成确定性的：请求已到 broker
    /// → seek → 放闸。旧实现没有刹车：FOUND + 一条 offset=2 的消息会把游标改成 3 并入库。
    /// </summary>
    [Fact]
    public void InFlightSeekWinsOverThePullResult()
    {
        // 处置顺序是声明的反序：cluster 先走（它要 join 还卡在闸上的连接线程），闸后走。
        using var gate = new ManualResetEventSlim(false);
        using var cluster = MockCluster.Start(1);
        cluster.ScriptPull(ResponseCode.Success, PullResp(next: 7),
            body: BodyWithOneMessage(queueOffset: 2), gate: gate);
        DefaultLitePullConsumer c = StartedLite(GroupPrefix + "SeekRace", cluster);
        try
        {
            Assert.True(WaitFor(() => Pulls(cluster) >= 1), "第一笔拉取到达 broker");
            MessageQueue mq = Queue0();
            c.Seek(mq, 99);
            gate.Set();

            Assert.True(WaitFor(() => PullsFrom(cluster, 99) > 0), "seek 之后下一笔 PULL_MESSAGE 从 99 起");
            Assert.Equal(99, c.PullCursorOf(mq));
            Assert.Empty(c.Poll(50));
        }
        finally
        {
            gate.Set();
            c.Shutdown();
        }
    }

    // ---------------------------------------------------------------- 4. 线上报文契约（#107）

    /// <summary>
    /// lite-pull 的每一笔拉取都必须走 LITE_PULL_MESSAGE(361) 且 sysFlag 与 Java
    /// `DefaultLitePullConsumerImpl#pullSyncImpl:1058` 的 buildSysFlag(false, block,
    /// true, false, litePull=true) 逐位一致（MQClientAPIImpl#pullMessage:816-820 就是
    /// 按这个位切码）。对照组：经典拉取（4 参 build，:248）仍是 11 且 lite 位为 0 ——
    /// 少了它，「361」可能只是所有 pull 都变成了 361。
    /// </summary>
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
                "lite 消费者的拉取用 LITE_PULL_MESSAGE(361)");
            Assert.Equal(0, cluster.CountRequests(RequestCode.PullMessage));

            int expectedFlag = PullSysFlag.BuildSysFlag(commitOffset: false, suspend: false,
                subscription: true, classFilter: false, litePull: true);
            foreach (WireRecord rec in cluster.Records().Where(r => r.Code == RequestCode.LitePullMessage))
            {
                int flag = int.Parse(rec.Ext["sysFlag"],
                    System.Globalization.CultureInfo.InvariantCulture);
                Assert.True(PullSysFlag.HasLitePullFlag(flag), "lite 位必须置起");
                Assert.False(PullSysFlag.HasSuspendFlag(flag), "lite 是短轮询，suspend 位为 0");
                Assert.Equal(expectedFlag, flag);
                // SUBSCRIPTION 位置起时表达式进报文（Java 的 makeCustomHeaderToNet 口径）
                Assert.Equal("*", rec.Ext["subscription"]);
            }

            // 对照：经典拉取消费者（DefaultMQPullConsumerImpl.pullSyncImpl:248 的 4 参版本）
            classic = new DefaultMQPullConsumer(GroupPrefix + "Classic");
            classic.SetNamesrvAddr(cluster.NamesrvAddr);
            classic.Start();
            classic.Pull(mq, "*", 0, 32, 3000);
            Assert.True(WaitFor(() => cluster.CountRequests(RequestCode.PullMessage) >= 1),
                "经典拉取用 PULL_MESSAGE(11)");
            WireRecord classicRec = cluster.Records().Last(r => r.Code == RequestCode.PullMessage);
            int classicFlag = int.Parse(classicRec.Ext["sysFlag"],
                System.Globalization.CultureInfo.InvariantCulture);
            Assert.False(PullSysFlag.HasLitePullFlag(classicFlag), "经典 pull 不带 lite 位");
            Assert.Equal(PullSysFlag.BuildSysFlag(false, false, true, false), classicFlag);
        }
        finally
        {
            c.Shutdown();
            classic?.Shutdown();
        }
    }
}
