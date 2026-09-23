// POP **消费侧**（DefaultMQPushConsumer.PopMode = true）本地单测，不需要集群。
//
// 为什么单独一个文件：协议管道（PopTests）过了 ≠ 消费循环对。消费侧有自己一套容易错的
// 语义，而且错得**很安静** —— 比如 ackIndex 默认值不对，CONSUME_SUCCESS 会一条都不 ack，
// 消息在 invisibleTime 到期后被 broker 复活重投；如果观测窗口比 invisibleTime 短，
// 真机看起来还是"全过"。所以能离线锁的必须先锁死。
//
// 覆盖：
//   - PopProcessQueue 计数与 dropped 语义
//   - POP_CK → (topic, brokerName, queueId, offset) 的还原，含 retry topic 反解
//   - isPopTimeout 判定
//   - 默认值与 Java 对齐（关掉时走原路径、poll < timeout、batchNums ≤ 32）
//   - 延迟档位表（单位秒）

using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;

using Xunit;

namespace RocketMQ.Client.Tests;

public class PopConsumerTests
{
    private const string Group = "GID_PopNetUnit";
    private const string Topic = "PopNetUnitTopic";
    private const string Broker = "broker-a";

    // 手工拼 8 段 CK：BuildExtraInfo 的 retryFlag 由 topic 推出，而这里要显式指定 retry 段
    private static string Ck(int queueId = 0, long offset = 0, string retry = "0",
                            long popTime = 1000, long invisible = 60000)
        => string.Format(System.Globalization.CultureInfo.InvariantCulture,
                         "0 {0} {1} 0 {2} {3} {4} {5}",
                         popTime, invisible, retry, Broker, queueId, offset);

    private static MessageExt Msg(string topic = Topic, int queueId = 0, long queueOffset = 0,
                                  string? popCk = null)
    {
        var m = new MessageExt { Topic = topic, QueueId = queueId, QueueOffset = queueOffset };
        if (popCk is not null)
        {
            m.Properties[MessageConst.PropertyPopCk] = popCk;
        }

        return m;
    }

    // ---------------------------------------------------------------- PopProcessQueue

    [Fact]
    public void ProcessQueueCountsUpAndDown()
    {
        var pq = new PopProcessQueue();
        Assert.Equal(0, pq.WaitAckCount());
        pq.IncFoundMsg(3);
        Assert.Equal(3, pq.WaitAckCount());
        pq.Ack();
        pq.Ack();
        Assert.Equal(1, pq.WaitAckCount());
        // Java 传负数（decFoundMsg(-size)），这里按"减多少"理解
        pq.DecFoundMsg(-1);
        Assert.Equal(0, pq.WaitAckCount());
    }

    [Fact]
    public void ProcessQueueDroppedFlag()
    {
        var pq = new PopProcessQueue();
        Assert.False(pq.IsDropped());
        pq.SetDropped(true);
        Assert.True(pq.IsDropped());
    }

    // ---------------------------------------------------------------- 默认值

    [Fact]
    public void PopModeOffByDefault()
    {
        // 关掉时必须完全走原来的 pull 路径，行为与改动前一致
        Assert.False(new DefaultMQPushConsumer(Group).PopMode);
    }

    [Fact]
    public void PopDefaultsMatchJava()
    {
        var c = new DefaultMQPushConsumer(Group);
        // Java popInvisibleTime=60000 / popBatchNums=32 / popThresholdForQueue=96
        Assert.Equal(60000, c.PopInvisibleTime);
        Assert.Equal(32, c.PopBatchNums);
        Assert.Equal(96, c.PopThresholdForQueue);
        // broker 侧 maxMsgNums > 32 会回 INVALID_PARAMETER
        Assert.True(c.PopBatchNums <= 32);
        // 长轮询挂起时长必须 < 请求超时，否则客户端先超时、每次都空转
        Assert.True(c.PopPollTimeMillis < c.PopTimeoutMillis);
    }

    [Fact]
    public void DelayLevelTableIsSecondsAndAscending()
    {
        // 档位表单位秒，首档 10s（send 的延迟档位首档是 1s，别混）
        Assert.Equal(10, PopDelayLevelDefaults.Table[0]);
        Assert.Equal(7200, PopDelayLevelDefaults.Table[^1]);
        for (int i = 1; i < PopDelayLevelDefaults.Table.Length; i++)
        {
            Assert.True(PopDelayLevelDefaults.Table[i - 1] < PopDelayLevelDefaults.Table[i]);
        }

        Assert.Equal(5000, DefaultMQPushConsumer.MinPopInvisibleTime);
        Assert.Equal(300000, DefaultMQPushConsumer.MaxPopInvisibleTime);
    }

    // ---------------------------------------------------------------- POP_CK 还原

    [Fact]
    public void CkTargetNormalTopicRoundtrip()
    {
        var c = new DefaultMQPushConsumer(Group);
        MessageExt m = Msg(Topic, 3, 0, Ck(3, 7));
        PopCkTarget? t = c.PopCkTarget(m);
        Assert.NotNull(t);
        Assert.Equal(Topic, t!.Topic);
        Assert.Equal(Broker, t.BrokerName);
        Assert.Equal(3, t.QueueId);
        Assert.Equal(7, t.Offset);
    }

    [Fact]
    public void CkTargetRetryFlagOneRebuildsRetryTopic()
    {
        // retryFlag=1 → 真实 topic 是 %RETRY%<group>_<topic>（V1 下划线）。
        // 即使消息上的 topic 已经被 resetRetryAndNamespace 还原过，只要 CK 里是 1，
        // ack 的目标 topic 也必须是带 %RETRY% 前缀的那个。
        var c = new DefaultMQPushConsumer(Group);
        MessageExt m = Msg(Topic, 1, 2, Ck(1, 2, "1"));
        PopCkTarget? t = c.PopCkTarget(m);
        Assert.NotNull(t);
        Assert.Equal("%RETRY%" + Group + "_" + Topic, t!.Topic);
        Assert.Equal(2, t.Offset);
    }

    [Fact]
    public void CkTargetRetryFlagTwoUsesPlusSeparator()
    {
        // V2 用 '+' 分隔（enableRetryTopicV2=true 时）
        var c = new DefaultMQPushConsumer(Group);
        MessageExt m = Msg(Topic, 0, 0, Ck(0, 0, "2"));
        PopCkTarget? t = c.PopCkTarget(m);
        Assert.NotNull(t);
        Assert.Equal("%RETRY%" + Group + "+" + Topic, t!.Topic);
    }

    [Fact]
    public void CkTargetMissingReturnsNull()
    {
        // 没有 CK → 放弃 ack，交给 broker 复活（不能抛）
        var c = new DefaultMQPushConsumer(Group);
        Assert.Null(c.PopCkTarget(Msg()));
    }

    [Fact]
    public void CkTargetShortSegmentsReturnsNull()
    {
        // 段数不足（7 段）→ 放弃 ack，不能抛
        var c = new DefaultMQPushConsumer(Group);
        string seven = ExtraInfoUtil.BuildExtraInfo(0, 1, 2, 3, Topic, Broker, 4);
        Assert.Null(c.PopCkTarget(Msg(Topic, 4, 0, seven)));
    }

    [Fact]
    public void CkTargetGarbageReturnsNull()
    {
        var c = new DefaultMQPushConsumer(Group);
        Assert.Null(c.PopCkTarget(Msg(Topic, 0, 0, "only two")));
    }

    // ---------------------------------------------------------------- 超时判定

    [Fact]
    public void UnparsableCkCountsAsTimeout()
    {
        // Java isPopTimeout：解析不出 popTime/invisibleTime 就按超时处理
        Assert.True(DefaultMQPushConsumer.IsPopTimeout(0, 0));
        Assert.True(DefaultMQPushConsumer.IsPopTimeout(0, 60000));
        Assert.True(DefaultMQPushConsumer.IsPopTimeout(1000, 0));
    }

    [Fact]
    public void WithinWindowIsNotTimeout()
    {
        long now = UtilAll.CurrentTimeMillis();
        Assert.False(DefaultMQPushConsumer.IsPopTimeout(now, 60000));
        Assert.True(DefaultMQPushConsumer.IsPopTimeout(now - 60001, 60000));
    }

    // ------------------------------------------------- POP 循环的 pullRT/pullTPS
    //
    // Java popMessage 的 PopCallback.onSuccess:556-563 有一处不对称：RT 在 FOUND 分支
    // **判空之前**就记，TPS 只按真正弹到的条数记。漏记或记反方向都是静默故障 ——
    // 消息照弹照 ack、消费完全正常，只有 307 的 statusTable（运维看板）一片 0，
    // 而看板上"这个消费者没在拉取"和"压根没起来"是两种完全不同的处置。

    private static PopResult Popped(int n)
    {
        var r = new PopResult { Status = PopStatus.Found };
        for (int i = 0; i < n; i++)
        {
            r.MsgFoundList.Add(Msg(Topic, i, i));
        }

        return r;
    }

    [Fact]
    public void RecordPopPullStats_FoundRecordsRtThenTps()
    {
        var stats = new ConsumerStatsManager();
        string key = Topic + "@" + Group;
        // 倒退 30ms：RT = now - began，正向断言 >0 不依赖调度精度
        long began = UtilAll.CurrentTimeMillis() - 30;

        DefaultMQPushConsumer.RecordPopPullStats(stats, Group, Topic, Popped(2), began);

        (long rtValue, long rtTimes) = stats.TopicAndGroupPullRT.Find(key)!.Snapshot();
        (long tpsValue, long tpsTimes) = stats.TopicAndGroupPullTPS.Find(key)!.Snapshot();
        Assert.Equal(1, rtTimes);
        Assert.True(rtValue >= 30, $"RT 应是本轮弹出耗时，实得 {rtValue}");
        Assert.Equal(1, tpsTimes);
        Assert.Equal(2, tpsValue);
    }

    [Fact]
    public void RecordPopPullStats_FoundWithEmptyListRecordsRtOnly()
    {
        // Java 在判空**之前**记 RT：FOUND 但 0 条也是"这一次确实拉了一回"
        var stats = new ConsumerStatsManager();
        string key = Topic + "@" + Group;
        long began = UtilAll.CurrentTimeMillis() - 30;

        DefaultMQPushConsumer.RecordPopPullStats(stats, Group, Topic, Popped(0), began);

        Assert.Equal(1, stats.TopicAndGroupPullRT.Find(key)!.Snapshot().times);
        // TPS 一格都不该动：0 条消息记进分子只会把平均值拉低
        Assert.Null(stats.TopicAndGroupPullTPS.Find(key));
    }

    [Fact]
    public void RecordPopPullStats_PollingNotFoundRecordsNothing()
    {
        // POP 的空轮询是常态（长轮询挂满 pollTime 后返回），算进 RT 等于用挂起时长
        // 稀释平均拉取耗时 —— 看板上的 pullRT 会完全失去意义。
        var stats = new ConsumerStatsManager();
        string key = Topic + "@" + Group;

        DefaultMQPushConsumer.RecordPopPullStats(
            stats, Group, Topic,
            new PopResult { Status = PopStatus.PollingNotFound, MsgFoundList = { Msg(Topic, 0, 0) } },
            UtilAll.CurrentTimeMillis() - 30);

        Assert.Null(stats.TopicAndGroupPullRT.Find(key));
        Assert.Null(stats.TopicAndGroupPullTPS.Find(key));
    }
}
