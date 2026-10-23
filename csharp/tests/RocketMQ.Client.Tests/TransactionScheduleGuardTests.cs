// Offline unit tests for the schedule guard on the transactional send path.
using System.Text;
using RocketMQ.Client;
using RocketMQ.Common;

namespace RocketMQ.Client.Tests;

public class TransactionScheduleGuardTests
{
    private const string Topic = "TxGuardTopic";

    private sealed class CommitListener : ITransactionListener
    {
        public LocalTransactionState ExecuteLocalTransaction(Message msg, string arg) =>
            LocalTransactionState.CommitMessage;

        public LocalTransactionState CheckLocalTransaction(MessageExt msg) =>
            LocalTransactionState.CommitMessage;
    }

    private static DefaultMQProducer UnstartedProducer() =>
        new("PG_TxGuard") { ClientId = "txguard-" + Guid.NewGuid().ToString("N") };

    private static readonly string[] ScheduleNames =
    {
        MessageConst.PropertyDelayTimeLevel,
        MessageConst.PropertyTimerDelaySec,
        MessageConst.PropertyTimerDelayMs,
        MessageConst.PropertyTimerDeliverMs,
    };

    [Theory]
    [MemberData(nameof(ScheduledValues))]
    public void ScheduledMessageIsRefused(string name, string value)
    {
        // A half message is committed exactly as it stands, so no schedule asked for here can
        // be honoured. Unlike the batch gate this one reads the key, not the value: a "0" or an
        // empty value asks for nothing and still refuses.
        var msg = new Message(Topic, Encoding.UTF8.GetBytes("x"));
        msg.Properties[name] = value;

        MQClientException err = Assert.Throws<MQClientException>(
            () => UnstartedProducer().SendMessageInTransaction(msg, new CommitListener()));

        Assert.Equal("Transactional messages do not support delayed delivery", err.Message);
        // The refusal happens before anything is stamped: the caller still holds an ordinary message.
        Assert.False(msg.Properties.ContainsKey(MessageConst.PropertyTransactionPrepared));
        Assert.False(msg.Properties.ContainsKey(MessageConst.PropertyProducerGroup));
    }

    public static TheoryData<string, string> ScheduledValues()
    {
        var data = new TheoryData<string, string>();
        foreach (string name in ScheduleNames)
        {
            foreach (string value in new[] { "3", "0", "" })
            {
                data.Add(name, value);
            }
        }
        return data;
    }

    [Theory]
    [InlineData("DELAY_TIME")]
    [InlineData("SOME_USER_PROPERTY")]
    public void OnlyTheFourScheduleNamesRefuse(string name)
    {
        // DELAY_TIME only looks like a schedule spelling and is not one; reaching the send path
        // here proves the guard let it through (the producer was never started, so that is where
        // it stops).
        var msg = new Message(Topic, Encoding.UTF8.GetBytes("x"));
        msg.Properties[name] = "3";

        MQClientException err = Assert.Throws<MQClientException>(
            () => UnstartedProducer().SendMessageInTransaction(msg, new CommitListener()));

        Assert.DoesNotContain("do not support delayed delivery", err.Message);
        Assert.Contains("not started", err.Message);
    }

    [Fact]
    public void PlainMessageReachesTheSendPath()
    {
        var msg = new Message(Topic, Encoding.UTF8.GetBytes("x"));

        MQClientException err = Assert.Throws<MQClientException>(
            () => UnstartedProducer().SendMessageInTransaction(msg, new CommitListener()));

        Assert.Contains("not started", err.Message);
    }
}
