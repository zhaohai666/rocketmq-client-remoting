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
        var c = new DefaultLitePullConsumer();
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
        var c = new DefaultLitePullConsumer();
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
