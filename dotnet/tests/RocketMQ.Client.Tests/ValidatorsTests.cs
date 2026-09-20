// 名字校验（Java Validators + TopicValidator）的离线对拍。
//
// 校验的价值在于「不打网络就失败」，所以除了规则表本身，这里还断言四个入口
// （Producer.Start / PushConsumer.Start / PullConsumer.Start / LitePullConsumer.Start）
// 会在任何 I/O 之前拒绝非法组名——测试全程没有可用的 name server，能抛出**组名**
// 错误而不是「name server address is not set」，本身就证明了顺序正确。
//
// 与 python/tests/test_validators.py、cpp/tests/test_validators.cpp、
// rust/src/client/validators_tests.rs 同题。
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class ValidatorsTests
{
    // ---------------------------------------------------------------- 字符表

    [Theory]
    // Java 白名单 ^[%|a-zA-Z0-9_-]+$：这四个符号是仅有的非字母数字合法字符
    [InlineData("order-topic")]
    [InlineData("order_topic")]
    [InlineData("CID_ONSAPI_PULL")]
    [InlineData("%RETRY%myGroup")]
    [InlineData("%DLQ%myGroup")]
    [InlineData("Topic|With|Pipe")]
    [InlineData("TOPIC_with_9_digits")]
    // 边界码点：'~'(126) 不在表里，同样非法
    [InlineData("topic~1")]
    [InlineData("topic.name")]
    [InlineData("topic/name")]
    [InlineData("topic:broker")]
    [InlineData("topic@host")]
    [InlineData("topic+1")]
    [InlineData("topic#1")]
    [InlineData("中文topic")]
    [InlineData("😀topic")]
    [InlineData("café")]
    [InlineData("tab\there")]
    [InlineData("line\nbreak")]
    public void CharTableMatchesJava(string name)
    {
        bool illegal = TopicValidator.IsTopicOrGroupIllegal(name);
        // 期望值直接由 Java 白名单正则表达：只有 % - _ | 与 ASCII 字母数字可用
        bool expected = System.Text.RegularExpressions.Regex.IsMatch(name, @"^[a-zA-Z0-9_%|\-]+$") == false;
        Assert.Equal(expected, illegal);
    }

    [Fact]
    public void EmptyStringIsNotIllegalBecauseCharsetIsCheckedLast()
    {
        // Java 的空串判定在 isBlank 那一步，字符表这一步对空串返回 false
        Assert.False(TopicValidator.IsTopicOrGroupIllegal(""));
        Assert.False(TopicValidator.IsTopicOrGroupIllegal(null));
    }

    [Fact]
    public void CodePointAbove127IsAlwaysIllegal()
    {
        // Java 的位表有 128 个槽位，但只给白名单字符置位：所以 DEL(0x7f) 这类
        // "小于 128 却不在表里" 的字符同样非法，而 0x80 是越界即非法。
        Assert.True(TopicValidator.IsTopicOrGroupIllegal("\u007f"));
        Assert.True(TopicValidator.IsTopicOrGroupIllegal("\u0080"));
        Assert.True(TopicValidator.IsTopicOrGroupIllegal("topic\u00e9"));
        Assert.False(TopicValidator.IsTopicOrGroupIllegal("topic|1"));
    }

    // ---------------------------------------------------------------- checkTopic

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("   ")]
    public void CheckTopicRejectsBlank(string? topic)
    {
        MQClientException e = Assert.Throws<MQClientException>(() => Validators.CheckTopic(topic));
        Assert.Equal("The specified topic is blank", e.Message);
    }

    [Fact]
    public void CheckTopicAllows127AndRejects128()
    {
        Validators.CheckTopic(new string('a', TopicValidator.TopicMaxLength));
        MQClientException e = Assert.Throws<MQClientException>(
            () => Validators.CheckTopic(new string('a', TopicValidator.TopicMaxLength + 1)));
        Assert.Equal("The specified topic is longer than topic max length 127.", e.Message);
    }

    [Fact]
    public void CheckTopicReportsPatternInErrorMessage()
    {
        MQClientException e = Assert.Throws<MQClientException>(() => Validators.CheckTopic("bad.topic"));
        Assert.Equal("The specified topic[bad.topic] contains illegal characters, allowing only "
                     + TopicValidator.ValidCharPattern, e.Message);
    }

    // ---------------------------------------------------------------- checkGroup

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData("\t ")]
    public void CheckGroupRejectsBlank(string? group)
    {
        MQClientException e = Assert.Throws<MQClientException>(() => Validators.CheckGroup(group));
        Assert.Equal("the specified group is blank", e.Message);
    }

    [Fact]
    public void CheckGroupAllows120AndRejects121()
    {
        // group 比 topic 短：它要参与拼 %RETRY%group_topic / %DLQ%group_topic
        Validators.CheckGroup(new string('g', TopicValidator.GroupMaxLength));
        MQClientException e = Assert.Throws<MQClientException>(
            () => Validators.CheckGroup(new string('g', TopicValidator.GroupMaxLength + 1)));
        Assert.Equal("the specified group[" + new string('g', 121)
                     + "] is longer than group max length: 120.", e.Message);
    }

    [Fact]
    public void CheckGroupRejectsIllegalChars()
    {
        MQClientException e = Assert.Throws<MQClientException>(() => Validators.CheckGroup("CID 001"));
        Assert.Equal("the specified group[CID 001] contains illegal characters, allowing only "
                     + TopicValidator.ValidCharPattern, e.Message);
    }

    // ---------------------------------------------------------------- checkMessage

    private const int MaxBody = 4096;

    private static Message Msg(string topic, byte[] body) => new() { Topic = topic, Body = body };

    [Fact]
    public void CheckMessageRejectsNullMessageWithCode13()
    {
        MQClientException e = Assert.Throws<MQClientException>(() => Validators.CheckMessage(null!, MaxBody));
        Assert.Equal("the message is null", e.Message);
        Assert.Equal(ResponseCode.MessageIllegal, e.ResponseCode);
    }

    [Fact]
    public void CheckMessageChecksTopicBeforeBody()
    {
        // 顺序是 Java 的行为：topic 非法 + body 为空时，报的是 topic 的问题
        Message m = Msg("bad.topic", Array.Empty<byte>());
        MQClientException e = Assert.Throws<MQClientException>(() => Validators.CheckMessage(m, MaxBody));
        Assert.Contains("bad.topic", e.Message);
        Assert.StartsWith("The specified topic[", e.Message);
    }

    [Fact]
    public void CheckMessageRejectsEmptyBodyWithCode13()
    {
        MQClientException e = Assert.Throws<MQClientException>(
            () => Validators.CheckMessage(Msg("T1", Array.Empty<byte>()), MaxBody));
        Assert.Equal("the message body length is zero", e.Message);
        Assert.Equal(ResponseCode.MessageIllegal, e.ResponseCode);
    }

    [Fact]
    public void CheckMessageRejectsOversizedBodyWithCode13()
    {
        Message m = Msg("T1", new byte[MaxBody + 1]);
        MQClientException e = Assert.Throws<MQClientException>(() => Validators.CheckMessage(m, MaxBody));
        Assert.Equal("the message body size over max value, MAX: 4096", e.Message);
        Assert.Equal(ResponseCode.MessageIllegal, e.ResponseCode);

        // 恰好等于阈值必须放行（Java 用的是 >，不是 >=）
        Validators.CheckMessage(Msg("T1", new byte[MaxBody]), MaxBody);
    }

    [Fact]
    public void CheckMessageRejectsBrokerInternalTopicsButAllowsRetryTopic()
    {
        foreach (string t in new[]
                 {
                     "SCHEDULE_TOPIC_XXXX", "RMQ_SYS_TRANS_HALF_TOPIC", "RMQ_SYS_TRANS_OP_HALF_TOPIC",
                     "TRANS_CHECK_MAX_TIME_TOPIC", "SELF_TEST_TOPIC", "OFFSET_MOVED_EVENT",
                     "RMQ_SYS_ROCKSDB_TRANS_HALF_TOPIC", "RMQ_SYS_ROCKSDB_TRANS_OP_HALF_TOPIC",
                 })
        {
            string topic = t;
            MQClientException e = Assert.Throws<MQClientException>(
                () => Validators.CheckMessage(Msg(topic, new byte[] { 1 }), MaxBody));
            Assert.Equal("Sending message to topic[" + topic + "] is forbidden.", e.Message);
            // Java 这条走 MQClientException(String, null)，responseCode 是 -1（纯客户端错误），
            // 不是 body 校验那四条的 13——混用会让上层按 broker 码分支时判错。
            Assert.NotEqual(ResponseCode.MessageIllegal, e.ResponseCode);
        }

        // %RETRY%group 是 sendMessageBack 的正常目标，绝不能被禁
        Validators.CheckMessage(Msg("%RETRY%myGroup", new byte[] { 1 }), MaxBody);
        Validators.CheckMessage(Msg("%DLQ%myGroup", new byte[] { 1 }), MaxBody);
    }

    [Fact]
    public void CheckMessageRejectsLmqPathWithFileSeparator()
    {
        string sep = System.IO.Path.DirectorySeparatorChar.ToString();
        Message m = Msg("T1", new byte[] { 1 });
        m.PutProperty(MessageConst.PropertyInnerMultiDispatch, "%DLQ%g1" + sep + "extra");
        MQClientException e = Assert.Throws<MQClientException>(() => Validators.CheckMessage(m, MaxBody));
        Assert.Equal("INNER_MULTI_DISPATCH %DLQ%g1" + sep
                     + "extra can not contains " + sep + " character", e.Message);
        Assert.Equal(ResponseCode.MessageIllegal, e.ResponseCode);

        // 正常 LMQ 路径（逗号分隔多个队列）放行
        Message ok = Msg("T1", new byte[] { 1 });
        ok.PutProperty(MessageConst.PropertyInnerMultiDispatch, "queueA,queueB");
        Validators.CheckMessage(ok, MaxBody);
    }

    // ---------------------------------------------------------------- 系统 topic

    [Fact]
    public void SystemTopicMatchesJavaRuleSet()
    {
        Assert.True(TopicValidator.IsSystemTopic("TBW102"));
        Assert.True(TopicValidator.IsSystemTopic("SCHEDULE_TOPIC_XXXX"));
        Assert.True(TopicValidator.IsSystemTopic("BenchmarkTest"));
        Assert.True(TopicValidator.IsSystemTopic("CHECKPOINT_TOPIC"));
        // 前缀 rmq_sys_ 命中即算系统 topic
        Assert.True(TopicValidator.IsSystemTopic("rmq_sys_anything"));
        Assert.False(TopicValidator.IsSystemTopic("RMQ_SYS_TRACE_TOPIC_X"));
        Assert.False(TopicValidator.IsSystemTopic("MyBusinessTopic"));

        MQClientException e = Assert.Throws<MQClientException>(
            () => Validators.IsSystemTopic("rmq_sys_watermark"));
        Assert.Equal("The topic[rmq_sys_watermark] is conflict with system topic.", e.Message);
    }

    // ---------------------------------------------------------------- 入口接线

    [Fact]
    public void ProducerStartRejectsIllegalGroupBeforeAnyIo()
    {
        // 没有配 name server：若校验排在地址检查之后，这里会抛 "name server address is not set"
        var prod = new DefaultMQProducer("bad group");
        MQClientException e = Assert.Throws<MQClientException>(prod.Start);
        Assert.Contains("contains illegal characters", e.Message);
        Assert.False(prod.IsStarted);
    }

    [Fact]
    public void ProducerStartRejectsReservedDefaultGroup()
    {
        var prod = new DefaultMQProducer();
        MQClientException e = Assert.Throws<MQClientException>(prod.Start);
        Assert.Equal("producerGroup can not equal DEFAULT_PRODUCER, please specify another one.", e.Message);
    }

    [Fact]
    public void PushConsumerStartRejectsReservedDefaultGroup()
    {
        var cons = new DefaultMQPushConsumer();
        MQClientException e = Assert.Throws<MQClientException>(cons.Start);
        Assert.Equal("consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.", e.Message);
    }

    [Fact]
    public void PushConsumerStartRejectsOversizedGroup()
    {
        var cons = new DefaultMQPushConsumer("G" + new string('g', 120));
        MQClientException e = Assert.Throws<MQClientException>(cons.Start);
        Assert.Contains("is longer than group max length: 120.", e.Message);
    }

    [Fact]
    public void PullConsumerStartRejectsIllegalGroup()
    {
        var cons = new DefaultMQPullConsumer("CID.illegal");
        MQClientException e = Assert.Throws<MQClientException>(cons.Start);
        Assert.Contains("contains illegal characters", e.Message);
    }

    [Fact]
    public void LitePullConsumerStartRejectsReservedDefaultGroup()
    {
        var cons = new DefaultLitePullConsumer();
        MQClientException e = Assert.Throws<MQClientException>(cons.Start);
        Assert.Equal("consumerGroup can not equal DEFAULT_CONSUMER, please specify another one.", e.Message);
    }

    [Fact]
    public void LegalNamesSurviveTheWholeStartupGateOrder()
    {
        // 合法组名 + 没配地址：必须走到地址检查，说明组名那一步确实放行了
        var prod = new DefaultMQProducer("NormalProducer");
        MQClientException e = Assert.Throws<MQClientException>(prod.Start);
        Assert.Equal("name server address is not set", e.Message);

        var cons = new DefaultMQPushConsumer("NormalConsumer");
        cons.Subscribe("NormalTopic", "*");
        cons.SetMessageListener(new EmptyListener());
        MQClientException e2 = Assert.Throws<MQClientException>(cons.Start);
        Assert.Equal("name server address is not set", e2.Message);
    }

    private sealed class EmptyListener : IMessageListenerConcurrently
    {
        public bool Orderly() => false;

        public ConsumeConcurrentlyStatus ConsumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext ctx)
            => ConsumeConcurrentlyStatus.ConsumeSuccess;
    }
}
