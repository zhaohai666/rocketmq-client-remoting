// The value of validation lies in "failing without touching the network", so besides the
// rule table itself this file also asserts that the four entry points
// (Producer.Start / PushConsumer.Start / PullConsumer.Start / LitePullConsumer.Start)
// reject an illegal group name before any I/O -- there is no usable name server during
// the test at all, so throwing the **group name** error rather than "name server address
// is not set" already proves the ordering is right.
using RocketMQ.Common;
using RocketMQ.Remoting.Protocol;
using Xunit;

namespace RocketMQ.Client.Tests;

public class ValidatorsTests
{
    // ---------------------------------------------------------------- Character table

    [Theory]
    [InlineData("order-topic")]
    [InlineData("order_topic")]
    [InlineData("CID_ONSAPI_PULL")]
    [InlineData("%RETRY%myGroup")]
    [InlineData("%DLQ%myGroup")]
    [InlineData("Topic|With|Pipe")]
    [InlineData("TOPIC_with_9_digits")]
    // Boundary code point: '~' (126) is not in the table either, so it is illegal too
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
    public void CharTableMatchesLegacy(string name)
    {
        bool illegal = TopicValidator.IsTopicOrGroupIllegal(name);
        bool expected = System.Text.RegularExpressions.Regex.IsMatch(name, @"^[a-zA-Z0-9_%|\-]+$") == false;
        Assert.Equal(expected, illegal);
    }

    [Fact]
    public void EmptyStringIsNotIllegalBecauseCharsetIsCheckedLast()
    {
        Assert.False(TopicValidator.IsTopicOrGroupIllegal(""));
        Assert.False(TopicValidator.IsTopicOrGroupIllegal(null));
    }

    [Fact]
    public void CodePointAbove127IsAlwaysIllegal()
    {
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
        // group is shorter than topic: it has to take part in building %RETRY%group_topic / %DLQ%group_topic
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
            Assert.NotEqual(ResponseCode.MessageIllegal, e.ResponseCode);
        }

        // %RETRY%group is the normal target of sendMessageBack, it must never be banned
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

        // The normal LMQ path (several queues separated by commas) is let through
        Message ok = Msg("T1", new byte[] { 1 });
        ok.PutProperty(MessageConst.PropertyInnerMultiDispatch, "queueA,queueB");
        Validators.CheckMessage(ok, MaxBody);
    }

    // ---------------------------------------------------------------- System topics

    [Fact]
    public void SystemTopicMatchesLegacyRuleSet()
    {
        Assert.True(TopicValidator.IsSystemTopic("TBW102"));
        Assert.True(TopicValidator.IsSystemTopic("SCHEDULE_TOPIC_XXXX"));
        Assert.True(TopicValidator.IsSystemTopic("BenchmarkTest"));
        Assert.True(TopicValidator.IsSystemTopic("CHECKPOINT_TOPIC"));
        // matching the rmq_sys_ prefix already counts as a system topic
        Assert.True(TopicValidator.IsSystemTopic("rmq_sys_anything"));
        Assert.False(TopicValidator.IsSystemTopic("RMQ_SYS_TRACE_TOPIC_X"));
        Assert.False(TopicValidator.IsSystemTopic("MyBusinessTopic"));

        MQClientException e = Assert.Throws<MQClientException>(
            () => Validators.IsSystemTopic("rmq_sys_watermark"));
        Assert.Equal("The topic[rmq_sys_watermark] is conflict with system topic.", e.Message);
    }

    // ---------------------------------------------------------------- Entry point wiring

    [Fact]
    public void ProducerStartRejectsIllegalGroupBeforeAnyIo()
    {
        // No name server configured: if validation came after the address check, this would throw "name server address is not set"
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
        // A legal group name plus no address: it must reach the address check, which shows the group name step really let it through
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
