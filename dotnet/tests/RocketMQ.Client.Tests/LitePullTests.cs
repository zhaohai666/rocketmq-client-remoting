// 轻量拉取消费者（DefaultLitePullConsumer）+ 220/221/309 body 单测
// （镜像 C++ test_lite_pull.cpp 的核心部分）。
//
// 不起网络：只测「start 之前」的纯状态行为与 221/309 应答体的 wire 形状
// （fastjson2 内联对象键 / null 字段）——broker/admin 端按 Java 语义解释这些 body。
using RocketMQ.Client;
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class LitePullTests
{
    /// <summary>这版 xunit 的 Assert.Equal 不带 because 参数，数值断言统一走这个包装。</summary>
    private static void Eq(long expected, long actual, string because) =>
        Assert.True(actual == expected, $"expected {expected}, actual {actual}, because {because}");

    // ---------------------------------------------- start 之前的纯状态行为

    [Fact]
    public void Constructor_DefaultGroup()
    {
        var c = new DefaultLitePullConsumer();
        Assert.Equal(MixAll.DefaultConsumerGroup, c.ConsumerGroup);
        Assert.False(c.IsStarted);
    }

    [Fact]
    public void Start_WithoutSubscriptionOrNamesrv_Throws()
    {
        var c = new DefaultLitePullConsumer();
        // start 前没配 namesrv：必须显式报错（不能静默起空转线程）
        Assert.ThrowsAny<MQClientException>(c.Start);
    }

    [Fact]
    public void ConsumeTimestamp_DefaultsToJavaWallClockFormat()
    {
        var c = new DefaultLitePullConsumer();
        // Java DefaultLitePullConsumer.java:168：默认 now-30min 的 14 位 yyyyMMddHHmmss
        Assert.Equal(14, c.ConsumeTimestamp.Length);
        Assert.All(c.ConsumeTimestamp, ch => Assert.True(char.IsDigit(ch)));
    }

    [Fact]
    public void Start_RejectsEpochLookingConsumeTimestamp()
    {
        // 组名要合法且不能是保留的 DEFAULT_CONSUMER：checkConfig 排在起点校验之前，
        // 用默认组只会测到组名那条错误。
        var c = new DefaultLitePullConsumer("LitePullTsGroup");
        c.SetNamesrvAddr("127.0.0.1:1");
        c.Subscribe("MyTopic", "*");
        // 纯数字的 epoch 毫秒必须被拒（旧实现按 epoch 解释，静默算出错位起点）
        c.SetConsumeTimestamp("1700000000000");
        var ex = Assert.Throws<MQClientException>(c.Start);
        Assert.Contains("consumeTimestamp is invalid", ex.Message);
    }

    [Fact]
    public void Start_KeepsValidConsumeTimestamp()
    {
        var c = new DefaultLitePullConsumer("LitePullTsGroup");
        c.SetNamesrvAddr("127.0.0.1:1");
        c.Subscribe("MyTopic", "*");
        c.SetConsumeTimestamp("20230101000000");
        // 合法值不能被这条启动守卫误杀（namesrv 不可达是另一回事）
        var ex = Record.Exception(c.Start);
        Assert.DoesNotContain("consumeTimestamp is invalid", ex?.Message ?? string.Empty);
        c.Shutdown();
    }

    [Fact]
    public void Subscribe_Assign_Seek_Poll_BeforeStart_DoesNotCrash()
    {
        var c = new DefaultLitePullConsumer();

        // subscribe（未启动也要能登记，Java 同语义）
        c.Subscribe("MyTopic", "TagA || TagB");

        // assign（未启动时 resolve initial offset 会失败但必须被吞掉，不能崩）
        var mq = new MessageQueue("MyTopic", "broker-a", 3);
        c.Assign(new[] { mq });
        Assert.Single(c.Assignment());
        Assert.Equal(3, c.Assignment()[0].QueueId);

        // 未启动 committed 必须回 -1（没有 client 可查）
        Assert.Equal(-1, c.Committed(mq));

        // poll 空缓冲：短超时返回空列表（不阻塞、不崩）
        Assert.Empty(c.Poll(20));

        // seek 未启动也要能登记位点（不查 broker）
        c.Seek(mq, 42);
        Assert.Single(c.Assignment());

        // pause / resume 未启动不崩
        c.Pause(new[] { mq });
        c.Resume(new[] { mq });

        // 重复 subscribe 覆盖旧表达式（Java 同语义：subscriptionTable.put）
        c.Subscribe("MyTopic", "TagC");
    }

    // ---------------------------------------------- 三张位点表（对位 C++ testLitePullOffsetTable）

    /// <summary>
    /// #68 的核心：拉取游标 / 已消费游标 / 内存位点表是三样东西，提交只能走第三样，
    /// 而 commitAll 的取数来源是**已消费**游标。这里全程不起网络，靠 seek 与 commit(map)
    /// 把三张表各自填满，再验守卫（-1、非本实例持有）、空入参、以及 persistAll 的
    /// "remove unused mq" 清理。
    /// </summary>
    [Fact]
    public void LitePullOffsetTable_ThreeTablesGuardsWithoutNetwork()
    {
        var c = new DefaultLitePullConsumer("LitePullOffsetTableGroup");
        var q0 = new MessageQueue("MyTopic", "broker-a", 0);
        var q1 = new MessageQueue("MyTopic", "broker-a", 1);
        var stranger = new MessageQueue("MyTopic", "broker-a", 7);
        c.Assign(new[] { q0, q1 });

        // 两条游标初值 -1（Java MessageQueueState 的 pullOffset/consumeOffset）：
        // 未 Start 时 assign 解析不了起点，正好让这个初值可观测。
        Assert.Equal(-1L, c.PullCursorOf(q0));
        Assert.Equal(-1L, c.ConsumeCursorOf(q0));

        // Commit()（无参 = Java commitAll）取的是已消费游标：一条都没交付 ⇒ 一格都不写
        c.Commit();
        Assert.Equal(-1L, c.Committed(q0));

        // Commit(map) 只写提交落点，两条游标一律不动
        c.Commit(new Dictionary<MessageQueue, long> { [q0] = 5, [q1] = 8 }, persist: false);
        Assert.Equal(5L, c.Committed(q0));
        Assert.Equal(8L, c.Committed(q1));
        Assert.Equal(-1L, c.PullCursorOf(q0));
        Assert.Equal(-1L, c.ConsumeCursorOf(q0));

        // 两道守卫：offset == -1（Java 的 log.error）+ 不是本实例持有的队列
        c.Commit(new Dictionary<MessageQueue, long> { [q0] = -1, [stranger] = 3 }, persist: false);
        Eq(5L, c.Committed(q0), "offset == -1 只记日志，不覆盖已有位点");
        Eq(-1L, c.Committed(stranger), "没分配到的队列不替它提交");

        // 空 map / 空集合：Java 都是直接 return，连表都不碰
        c.Commit(new Dictionary<MessageQueue, long>(), persist: false);
        Eq(5L, c.Committed(q0), "空 map 忽略这次提交");
        c.Commit(Array.Empty<MessageQueue>(), persist: false);
        Eq(5L, c.Committed(q0), "空集合忽略这次提交");

        // Commit(Set) 取的是已消费游标，不是任意指定值：没交付过 ⇒ 守卫拦住
        c.Commit(new[] { q0 }, persist: false);
        Eq(5L, c.Committed(q0), "Commit(Set) 在没有交付记录时不写 -1");

        // seek 同时改写两条游标（Java nextPullOffset 吃掉 seekOffset 时连 consumeOffset 一起改）
        c.Seek(q0, 2);
        Assert.Equal(2L, c.PullCursorOf(q0));
        Eq(2L, c.ConsumeCursorOf(q0), "seek 也要改已消费游标，否则重放的段会被旧位点跳过");

        // assign 缩范围：撤掉的队列连着两条游标一起丢（Java updateAssignedMessageQueue），
        // 但内存位点表**不**清 —— 那份清理挂在 subscribe 模式的 rebalance 上（Java 同）。
        c.Assign(new[] { q0 });
        Assert.Equal(-1L, c.PullCursorOf(q1));
        Assert.Equal(-1L, c.ConsumeCursorOf(q1));
        Eq(8L, c.Committed(q1), "assign 模式不碰 offsetStore");
        c.Commit(new Dictionary<MessageQueue, long> { [q1] = 99 }, persist: false);
        Eq(8L, c.Committed(q1), "撤掉的队列不替它改位点");

        // Java RemoteBrokerOffsetStore#persistAll 的 "remove unused mq"：点名提交只发被点名的
        // 队列，内存表里**其余**条目顺手删掉（这里没 Start()，网络那半段自然跳过）。
        c.Commit(new[] { q0 }, persist: true);
        Eq(2L, c.Committed(q0), "Commit(Set) 提交的是已消费游标");
        Eq(-1L, c.Committed(q1), "persistAll 会把没点名的队列从内存表里丢掉");
        Eq(2L, c.PendingCommitOf(q0), "点名的队列留在表里");
    }

    // ---------------------------------------------- 221 GetConsumerStatusBody

    [Fact]
    public void GetConsumerStatusBody_WireShape()
    {
        var b = new GetConsumerStatusBody();
        b.MessageQueueTable[new MessageQueue("MyTopic", "broker-a", 3)] = 42;
        b.MessageQueueTable[new MessageQueue("MyTopic", "broker-a", 1)] = 7;

        string json = System.Text.Encoding.UTF8.GetString(b.Encode());
        // 键是 fastjson2 风格的 MessageQueue 内联 JSON（字母序：brokerName, queueId, topic），
        // 作为**转义字符串键**序列化（与已真机验证过的 307 MqTable 同款编码）
        Assert.Contains(
            "\"{\\\"brokerName\\\":\\\"broker-a\\\",\\\"queueId\\\":3,\\\"topic\\\":\\\"MyTopic\\\"}\":42",
            json);
        // Java 保留的废弃字段 consumerTable 必须带（空对象）
        Assert.Contains("\"consumerTable\":{}", json);
        Assert.Contains("\"messageQueueTable\":", json);
    }

    // ---------------------------------------------- 309 ConsumeMessageDirectlyResult

    [Fact]
    public void ConsumeMessageDirectlyResult_DefaultWireShape()
    {
        // 默认值对齐 Java：order=false / autoCommit=true；空字段序列化成 null
        var def = new ConsumeMessageDirectlyResult();
        Assert.False(def.Order);
        Assert.True(def.AutoCommit);

        string json = System.Text.Encoding.UTF8.GetString(def.Encode());
        Assert.Contains("\"order\":false", json);
        Assert.Contains("\"autoCommit\":true", json);
        Assert.Contains("\"consumeResult\":null", json);
        Assert.Contains("\"remark\":null", json);
        Assert.Contains("\"spentTimeMills\":0", json);
    }

    [Fact]
    public void ConsumeMessageDirectlyResult_RoundTrip()
    {
        var ok = new ConsumeMessageDirectlyResult { ConsumeResult = "CR_SUCCESS", SpentTimeMills = 12 };
        Assert.Contains("\"consumeResult\":\"CR_SUCCESS\"",
            System.Text.Encoding.UTF8.GetString(ok.Encode()));

        var bad = new ConsumeMessageDirectlyResult
        {
            Order = true,
            ConsumeResult = "CR_THROW_EXCEPTION",
            Remark = "std::exception: boom",
            SpentTimeMills = 33,
        };
        Assert.True(ConsumeMessageDirectlyResult.Decode(bad.Encode(), out ConsumeMessageDirectlyResult back));
        Assert.True(back.Order);
        Assert.True(back.AutoCommit);   // 往返保持默认 true
        Assert.Equal("CR_THROW_EXCEPTION", back.ConsumeResult);
        Assert.Equal("std::exception: boom", back.Remark);
        Assert.Equal(33, back.SpentTimeMills);
    }
}
